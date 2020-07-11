# This function creates the argument list and launches job managers


import argparse  # for command-line arguments
import os  # for file operations
import socket  # for netowrk hostname
import subprocess  # for launching detached processes on a local PC
import sys  # to set exit codes
import shutil

import numpy as np

from constants import version

DEDICATED_DEFAULT_JOBS_COUNT = 204
COMMON_DEFAULT_JOBS_COUNT = 2800

# Define arguments
arg_parser = argparse.ArgumentParser(
    description='Job manager. You must choose whether to resume simulations or restart and regenerate the arguments file')
arg_parser.add_argument('args_file', action='store', nargs='?', type=str, default='arguments.dat')
arg_parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + str(version))

arg_parser.add_argument('-d', '--dedicated', dest='dedicated', type=int, action='store', nargs='?',
                        default=0, const=DEDICATED_DEFAULT_JOBS_COUNT,
                        help='Number of jobs to launch on the dedicated partition.')
arg_parser.add_argument('-c', '--common', dest='common', action='store', nargs='?', type=int,
                        default=0, const=COMMON_DEFAULT_JOBS_COUNT,
                        help='Number of jobs to launch on the common partition.')

# Identify the system where the code is running
script_folder = 'job_manager'
script_name_common = 'sbatch_tars_common.sh'
hostname = socket.gethostname()
if hostname.startswith('tars-submit'):
    script_name_dedicated = 'sbatch_tars.sh'
if hostname.startswith('maestro-submit'):
    script_name_dedicated = ''
    script_name_common = 'sbatch_tars_maestro.sh'
    python_name = 'python3'
elif hostname == 'patmos':
    script_name_dedicated = 'sbatch_t_bayes.sh'
elif hostname == 'onsager-dbc':
    script_name_dedicated = 'job_manager.py'
    python_name = 'python3'
else:
    print(f'Unexpected hostname `{hostname}`.'
          'Unable to choose the right code version to launch. Aborting.')
    exit()

# Analyze if need to restart or resume
input_args = arg_parser.parse_args()
args_file = input_args.args_file
# Convert to a number if argument is parsed as list
jobs_common = input_args.common[0] if isinstance(input_args.common, list) else \
    input_args.common
jobs_dedicated = input_args.dedicated[0] if isinstance(input_args.dedicated, list) else \
    input_args.dedicated

if not (jobs_dedicated + jobs_common):
    arg_parser.error('At least one of the arguments {-c, -d} is required.')

# Count the number of lines in the arguments file
with open(args_file, 'r') as f:
    for i, _ in enumerate(f):
        pass
max_lines = i + 1


def get_position_file(args_file):
    args_basename = os.path.splitext(os.path.basename(args_file))[0]
    position_file = args_basename + ".pos"
    return position_file


if args_file == 'arguments.dat':
    # Find the next free arguments file name
    args_files_limit = 10000
    args_file_base, ext = os.path.splitext(os.path.basename(args_file))


    def get_args_file(i):
        return args_file_base + f'_{i:d}' + ext


    success = False
    for i in range(args_files_limit):
        try:
            with open(get_args_file(i), 'r'):
                pass
        except FileNotFoundError:
            success = True
            break

    if not success:
        raise RuntimeError('No free arguments file names left. Please clean up.')

    # Copy data
    new_args_file = get_args_file(i)
    shutil.copy(args_file, new_args_file)
    try:
        os.unlink(get_position_file(new_args_file))
    except FileNotFoundError:
        pass
    print(f'Arguments file copied to {new_args_file}')
else:
    new_args_file = args_file
    print(f'Resuming from the arguments file "{new_args_file}"')

workers_count = jobs_common + jobs_dedicated

# Launch job managers
if script_name_dedicated == 'job_manager.py':
    cmd_str = '{python_name} %s' % (script_name_dedicated)
    popens = []
    pids = []
    for j in range(1, jobs_count + 1):
        cur_popen = subprocess.Popen([python_name, script_name_dedicated + " " + new_args_file])
        popens.append(cur_popen)
        pids.append(cur_popen.pid)
    print("Launched %i local job managers" % (jobs_count))
    print("PIDs: ")
    print(pids)

    # Collect exit codes
    for j in range(jobs_count):
        popens[j].wait()
    print("All job managers finished successfully")

else:
    # Launch calculations on each partition
    shift = 0
    for jobs, script in zip([jobs_dedicated, jobs_common],
                            [script_name_dedicated, script_name_common]):
        if jobs:
            cmd_str = f'sbatch --array=1-{jobs:d} {script} ' \
                      f'{new_args_file} {workers_count:d} {shift:d}'
            os.system(cmd_str)
            print(f'Submitted {jobs} jobs to "dbc_pmo". Processing: {new_args_file}.')
            shift += jobs

    # if jobs_dedicated:  # Launch on TARS's dedicated partition
    #     cmd_str = f'sbatch --array=1-{jobs_dedicated:d} {script_name_dedicated} {new_args_file} ' \
    #               f'{workers_count:d} {shift:d}'
    #     os.system(cmd_str)
    #     print(f'Submitted {jobs_dedicated} jobs to "dbc_pmo". Processing: {new_args_file}.')
    #
    # shift = jobs_dedicated
    # if jobs_common:    # Launch on TARS's common partition
    #     cmd_str = f'sbatch --array=1-{jobs_common:d} {script_name_common} ' \
    #               f'{new_args_file} {workers_count:d} {shift:d}'
    #     os.system(cmd_str)
    #     print(f'Submitted {jobs_common} jobs to "common". Processing: {new_args_file}.')
