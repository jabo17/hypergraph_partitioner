import subprocess
import argparse
import sys
import os
import os.path
from threading import Timer
import signal
import shlex
import ntpath
import shutil

#######################################
# Common functionality for Mt-KaHyPar #
#######################################

invalid = 2147483647

# Manage the result via a global. A bit ugly, but should be OK for a script
_result_values = {
  "timeout": "no",
  "failed": "no",
}
_result_initialized = False


def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument("graph", type=str)
  parser.add_argument("threads", type=int)
  parser.add_argument("k", type=int)
  parser.add_argument("epsilon", type=float)
  parser.add_argument("seed", type=int)
  parser.add_argument("objective", type=str)
  parser.add_argument("timelimit", type=int)
  parser.add_argument("--partition_folder", type=str, default = "")
  parser.add_argument("--name", type=str, default = "")
  parser.add_argument("--args", type=str, default = "")
  parser.add_argument("--header", type=str, default = "")
  parser.add_argument("--tag", action="store_true")

  return parser.parse_args()


def run_mtkahypar(mt_kahypar, args, default_args, print_fail_msg=True, detect_instance_type=False):
  args_list = shlex.split(args.args)

  for arg_key in default_args:
    assert ("--" in arg_key) and not ("=" in arg_key), f"Invalid default argument: {arg_key}"
    if not any(a for a in args_list if (arg_key in a)):
      args_list.append(f"{arg_key}={default_args[arg_key]}")

  if detect_instance_type:
    for arg in args_list:
      assert not ("instance-type" in arg) and not ("input-file-format" in arg)
    if args.graph.endswith(".metis") or args.graph.endswith(".graph"):
      args_list.append("--instance-type=graph")
      args_list.append("--input-file-format=metis")

  # Run Mt-KaHyPar
  cmd = [mt_kahypar,
         "-h" + args.graph,
         "-k" + str(args.k),
         "-e" + str(args.epsilon),
         "--seed=" + str(args.seed),
         "-o" + str(args.objective),
         "-mdirect",
         "--s-num-threads=" + str(args.threads),
         "--verbose=false",
         "--sp-process=true",
         "--show-detailed-timings=true",
         *args_list]
  if args.partition_folder != "":
    cmd.extend(["--write-partition-file=true"])
    cmd.extend(["--partition-output-folder=" + args.partition_folder])
  mt_kahypar_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True, preexec_fn=os.setsid)

  # handle early interrupt cases where the Mt-KaHyPar process should be killed
  def kill_proc(*args):
    os.killpg(os.getpgid(mt_kahypar_proc.pid), signal.SIGTERM)

  signal.signal(signal.SIGINT, kill_proc)
  signal.signal(signal.SIGTERM, kill_proc)

  t = Timer(args.timelimit, kill_proc)
  t.start()
  out, err = mt_kahypar_proc.communicate()
  t.cancel()

  if mt_kahypar_proc.returncode == 0:
    # Extract metrics out of Mt-KaHyPar output
    for line in out.split('\n'):
      result_line = str(line).strip()
      if "RESULT" in result_line:
        return result_line, True
    assert False, "No result line found!"
  elif mt_kahypar_proc.returncode == -signal.SIGTERM:
    _result_values["timeout"] = "yes"
    return out, False
  else:
    _result_values["failed"] = "yes"
    if err and print_fail_msg:
      print(err, file=sys.stderr)
    return out, False


# for debugging
def print_call(mt_kahypar, args, default_args, detect_instance_type=False):
  args_list = shlex.split(args.args)

  for arg_key in default_args:
    assert ("--" in arg_key) and not ("=" in arg_key), f"Invalid default argument: {arg_key}"
    if not any(a for a in args_list if (arg_key in a)):
      args_list.append(f"{arg_key}={default_args[arg_key]}")

  if detect_instance_type:
    for arg in args_list:
      assert not ("instance-type" in arg) and not ("input-file-format" in arg)
    if args.graph.endswith(".metis") or args.graph.endswith(".graph"):
      args_list.append("--instance-type=graph")
      args_list.append("--input-file-format=metis")

  # Run Mt-KaHyPar
  cmd = [mt_kahypar,
         "-h" + args.graph,
         "-k" + str(args.k),
         "-e" + str(args.epsilon),
         "--seed=" + str(args.seed),
         "-o" + str(args.objective),
         "-mdirect",
         "--s-num-threads=" + str(args.threads),
         "--verbose=false",
         "--sp-process=true",
         "--show-detailed-timings=true",
         *args_list]
  print(shlex.join(cmd))
  return None, None


###############################
# Result parsing and printing #
###############################

def set_result_vals(**kwargs):
  global _result_values, _result_initialized
  _result_values.update(kwargs)
  _result_initialized = True


def parse(result_line, key, *, out=None, parser=float):
  assert _result_initialized, "set_result_vals must be called before parsing"
  assert parse != bool, "Parsing via bool does not work"
  if out is None:
    out = key
  assert out in _result_values, f"Tried to parse into {out}, which is not in the setup."

  if f" {key}=" in result_line:
    _result_values[out] = parser(result_line.split(f" {key}=")[1].split(" ")[0])
    return True
  return False


def parse_or_default(result_line, key, default, *, out=None, parser=float):
  if not parse(result_line, key, out=out, parser=parser):
    if out is None:
      out = key
    _result_values[out] = default


def parse_required_value(result_line, key, *, out=None, parser=float):
  if not parse(result_line, key, out=out, parser=parser):
    assert False, f"Required key {key} not contained in result line!"

def print_result(algorithm, args):
  assert _result_initialized, "set_result_vals must be called before print_result"
  timeout = _result_values["timeout"]
  failed = _result_values["failed"]
  imbalance = _result_values["imbalance"]
  total_time = _result_values["total_time"]
  km1 = _result_values["km1"]
  cut = _result_values["cut"]
  del _result_values["timeout"]
  del _result_values["failed"]
  del _result_values["imbalance"]
  del _result_values["total_time"]
  del _result_values["km1"]
  del _result_values["cut"]
  print(algorithm,
        ntpath.basename(args.graph),
        timeout,
        args.seed,
        args.k,
        args.epsilon,
        args.threads,
        imbalance,
        total_time,
        args.objective,
        km1,
        cut,
        failed,
        # note: the iteration order of a dict matches the insertion order
        # (guaranteed since python 3.7)
        *_result_values.values().__iter__(),
        sep=",")

  if args.header != "":
    header = ["algorithm", "graph", "timeout", "seed", "k", "epsilon", "num_threads", "imbalance", "totalPartitionTime", "objective", "km1", "cut", "failed"]
    if args.tag:
      header.insert(0, "tag")
    header.extend(_result_values.keys())
    with open(args.header, "w") as header_file:
      header_file.write(",".join(header) + "\n")

  if args.partition_folder != "":
    src_partition_file = args.partition_folder + "/" + ntpath.basename(args.graph) + ".part" + str(args.k) + ".epsilon" + str(args.epsilon) + ".seed" + str(args.seed) + ".KaHyPar"
    dst_partition_file = args.partition_folder + "/" + ntpath.basename(args.graph) + ".part" + str(args.k) + ".epsilon" + str(args.epsilon) + ".seed" + str(args.seed) + ".partition"
    if os.path.exists(src_partition_file):
      shutil.move(src_partition_file, dst_partition_file)
