#!/usr/bin/python3
import json
import argparse
import datetime
import os
import os.path
import ntpath
import shutil
import re

from experiments.partitioner_mapping import partitioner_mapping

partitioner_script_folder = os.environ.get("PARTITIONER_SCRIPT_FOLDER")
assert (partitioner_script_folder != None), "check env.sh"


###################################
###      Helper functions       ###
###################################

def intersection(lst1, lst2):
  lst3 = [value for value in lst1 if value in lst2]
  return lst3

def get_all_hypergraph_instances(dir):
  return [dir + "/" + hg for hg in os.listdir(dir) if hg.endswith('.hgr') or hg.endswith('.hmetis')]

def get_all_zoltan_instances(dir):
  return [dir + "/" + zoltan_hg for zoltan_hg in os.listdir(dir) if zoltan_hg.endswith('.zoltan.hg')]

def get_all_graph_instances(dir):
  return [dir + "/" + graph for graph in os.listdir(dir) if graph.endswith('.graph') or graph.endswith('.metis')]

def get_all_scotch_instances(dir):
  return [dir + "/" + graph for graph in os.listdir(dir) if graph.endswith('.scotch')]

def get_all_benchmark_instances_in_directory(input_format, instance_dir):
  if input_format == "hmetis" or input_format == "patoh":
    return get_all_hypergraph_instances(instance_dir)
  elif input_format == "zoltan":
    return get_all_zoltan_instances(instance_dir)
  elif input_format == "graph" or input_format == "metis":
    return get_all_graph_instances(instance_dir)
  elif input_format == "scotch":
    return get_all_scotch_instances(instance_dir)


def get_all_benchmark_instances(partitioner, config):
  input_format_list = partitioner_mapping[partitioner].format
  result = {}
  for format_type in input_format_list:
    if format_type == "graph":
        format_type = "metis"
    input_format = format_type + "_instance_folder"
    if input_format in config:
      assert "instances" not in config
      instance_dir = config[input_format]
      result.update({instance: None for instance in get_all_benchmark_instances_in_directory(format_type, instance_dir)})

    elif "instances" in config:
      # more general case where multiple directories and tags can be defined
      dir_list = [(entry["path"], entry["type"], entry.get("tag")) for entry in config["instances"]]
      for (instance_dir, curr_format, instance_tag) in dir_list:
        if curr_format == "graph":
            curr_format = "metis"
        assert curr_format in ["hmetis", "patoh", "zoltan", "metis", "scotch"], f"invalid instance type: {curr_format}"
        if curr_format == format_type:
          input_format = format_type + "_instance_folder"
          tmp = {instance: instance_tag for instance in get_all_benchmark_instances_in_directory(format_type, instance_dir)}
          intersection = {ntpath.basename(p) for p in result} & {ntpath.basename(p) for p in tmp}
          assert len(intersection) == 0, f"instance appears in multiple folders: {intersection}"
          result.update({instance: instance_tag for instance in tmp})

  assert len(result) > 0, f"No instances found for: {partitioner}"
  assert all(tag is not None for tag in result.values()) or all(tag is None for tag in result.values()), "Inconsistent instance tags!"
  result = [(graph, tag, k) for k in config["k"] for graph, tag in result.items()]

  if "instance_restriction" in config:
    instance_set = set()
    with open(config["instance_restriction"]) as r_file:
      for line in r_file.readlines():
        [graph, k] = line.split(",")
        instance_set.add((ntpath.basename(graph), int(k)))
    result = [(graph, tag, k) for graph, tag, k in result if (ntpath.basename(graph), k) in instance_set]

  return result

def serial_partitioner_call(partitioner, instance, k, epsilon, seed, objective, timelimit):
  return (
    partitioner_script_folder + "/" + partitioner_mapping[partitioner].script + ".py " + instance
    + " " + str(k) + " " + str(epsilon) + " " + str(seed) + " " + str(objective) + " " + str(timelimit)
  )

def parallel_partitioner_call(partitioner, instance, threads, k, epsilon, seed, objective, timelimit):
  return (
    partitioner_script_folder + "/" + partitioner_mapping[partitioner].script + ".py " + instance
    + " " + str(threads) + " " + str(k) + " " + str(epsilon) + " " + str(seed) + " " + str(objective) + " " + str(timelimit)
  )

def partitioner_call(is_serial, partitioner, instance, threads, k, epsilon, seed, objective, timelimit, config_file, algorithm_name, args, header, tag):
  if is_serial:
    call = serial_partitioner_call(partitioner, instance, k, epsilon, seed, objective, timelimit)
  else:
    call = parallel_partitioner_call(partitioner, instance, threads, k, epsilon, seed, objective, timelimit)
  if config_file != "":
    call += " --config " + config_file
  if algorithm_name != "":
    call += " --name " + algorithm_name
  if args is not None:
    assert "'" not in args
    call += f" --args ' {args}'"
  if header is not None:
    call += f" --header '{header}'"
    if tag is not None:
      call += " --tag"
  if tag is not None:
    call += f' | {{ line=$(cat); echo "{tag},$line"; }}'  # bash snippet which prepends to stdin
  return call

def partitioner_dump(result_dir, instance, threads, k, seed):
  return os.path.abspath(result_dir) + "/" + ntpath.basename(instance) + "." + str(threads) + "." + str(k) + "." + str(seed) + ".results"

def partitioner_header(result_dir):
  return str(os.path.abspath(result_dir)).removesuffix("_results") + ".header.csv"



###################################
###        Main Script          ###
###################################

parser = argparse.ArgumentParser()
parser.add_argument("experiment", type=str)
parser.add_argument("-f", "--force", action="store_true")

args = parser.parse_args()

with open(args.experiment) as json_experiment:
  config = json.load(json_experiment)

now = datetime.datetime.now()
experiment_dir = str(now.year) + "-" + str(now.month) + "-" + str(now.day) + "_" + config["name"]
workload_file = experiment_dir + "/workload.txt"
if args.force:
  shutil.rmtree(experiment_dir, ignore_errors=True)
  os.makedirs(experiment_dir, exist_ok=True)
else:
  try:
    os.makedirs(experiment_dir, exist_ok=False)
  except OSError:
    print("Experiment directory already exists! Call with -f to delete old directory")
    exit(1)

epsilon = config["epsilon"]
objective = config["objective"]
timelimit = config["timelimit"]
write_partition_file = config["write_partition_file"] if "write_partition_file" in config else False
dynamic_header = config["dynamic_header"] if "dynamic_header" in config else True

# Setup experiments
try:
  for partitioner_config in config["config"]:
    partitioner = partitioner_config["partitioner"]
    algorithm_file = partitioner
    if "name" in partitioner_config:
      algorithm_file = partitioner_config["name"]
    algorithm_file = '_'.join(list(map(lambda x: x.lower(), re.split(' |-', algorithm_file))))
    result_dir = experiment_dir + "/" + algorithm_file + "_results"
    os.makedirs(result_dir, exist_ok=True)

  for seed in config["seeds"]:
    for partitioner_config in config["config"]:
      partitioner = partitioner_config["partitioner"]
      algorithm_file = partitioner
      if "name" in partitioner_config:
        algorithm_file = partitioner_config["name"]
      algorithm_file = '_'.join(list(map(lambda x: x.lower(), re.split(' |-', algorithm_file))))
      result_dir = experiment_dir + "/" + algorithm_file + "_results"

      is_serial_partitioner = not partitioner_mapping[partitioner].parallel
      config_file = ""
      if "config_file" in partitioner_config:
        config_file = partitioner_config["config_file"]
      algorithm_name = '"' + partitioner + '"'
      if "name" in partitioner_config:
        algorithm_name = '"' + partitioner_config["name"] + '"'
      args = None
      if "args" in partitioner_config:
        args = partitioner_config["args"]
      header = None
      if dynamic_header and partitioner_mapping[partitioner].dynamic_header:
        header = partitioner_header(result_dir)

      partitioner_calls = []
      for instance, tag, k in get_all_benchmark_instances(partitioner, config):
        for threads in config["threads"]:
          if is_serial_partitioner and threads > 1 and len(config["threads"]) > 1:
            continue
          call = partitioner_call(is_serial_partitioner, partitioner, instance, threads, k, epsilon, seed, objective, timelimit, config_file, algorithm_name, args, header, tag)
          header = None
          if write_partition_file:
            call += " --partition_folder=" + os.path.abspath(result_dir)
          call += " >> " + partitioner_dump(result_dir, instance, threads, k, seed)
          partitioner_calls.append(call)

      # Write partitioner calls to workload file
      with open(experiment_dir + "/" + algorithm_file + "_workload.txt", "a") as partitioner_workload_file:
        partitioner_workload_file.write("\n".join(partitioner_calls))
        partitioner_workload_file.write("\n")

      with open(workload_file, "a") as global_workload_file:
        global_workload_file.write("\n".join(partitioner_calls))
        global_workload_file.write("\n")

except AssertionError as e:
  shutil.rmtree(experiment_dir, ignore_errors=True)
  raise e
except FileNotFoundError as e:
  shutil.rmtree(experiment_dir, ignore_errors=True)
  raise e

