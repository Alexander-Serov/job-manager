# This function creates the argument list and launches job managers


import argparse  # for command-line arguments
import os  # for file operations
import socket  # for netowrk hostname
import subprocess  # for launching detached processes on a local PC
import sys  # to set exit codes

import numpy as np

from constants import *

# Define arguments
arg_parser = argparse.ArgumentParser(
    description='Job manager. You must choose whether to resume simulations or restart and regenerate the arguments file')
arg_parser.add_argument('args_file', action='store', nargs='?', type=str, default='arguments.dat')
arg_parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + str(version))
arg_parser.add_argument('--common', dest='common', action='store_true')

mode_group = arg_parser.add_mutually_exclusive_group(required=True)
mode_group.add_argument('--restart', action='store_true')
mode_group.add_argument('--resume', action='store_true')

# dedicated_group = arg_parser.add_mutually_exclusive_group()
# dedicated_group.add_argument('--dedicated', dest='dedicated', action='store_true')
arg_parser.add_argument('--no-dedicated', dest='dedicated', action='store_false')

arg_parser.set_defaults(common=False, dedicated=True)

# Identify the system where the code is running
script_folder = 'job_manager'
script_name_common = 'sbatch_tars_common.sh'
hostname = socket.gethostname()
if hostname.startswith('tars-submit'):
    script_name = 'sbatch_tars.sh'
    jobs_count = jobs_count_tars
elif hostname == 'patmos':
    script_name = 'sbatch_t_bayes.sh'
    jobs_count = jobs_count_t_bayes
elif hostname == 'onsager-dbc':
    script_name = 'job_manager.py'
    python_name = 'python3'
    jobs_count = jobs_count_onsager
else:
    print('Unidentified hostname "' + hostname +
          '". Unable to choose the right code version to launch. Aborting.')
    exit()

# Analyze if need to restart or resume
input_args = arg_parser.parse_args()
bl_restart = input_args.restart
args_file = input_args.args_file

# args_file_main = args_file
# args_file_base = os.path.splitext(os.path.basename(args_file_main))[0]
#
#
# # # If restart is required, regenerate all files
# # if bl_restart:
# #     print("Creating arguments list...")
# #
# #     # Clear the arguments file
# #     try:
# #         os.remove(args_file)
# #     except:
# #         pass
# #
# #     # Clean slurm files in the root folder
# #     cmd = "rm -fv ./slurm-*"
# #     try:
# #         os.system(cmd)
# #     except Exception as e:
# #         print(e)
# #
# #     # Clean the logs and output folders
# #     for folder in (logs_folder, output_folder):
# #         if os.path.isdir(folder):
# #             print("Cleaning up the folder: '" + folder + "'.")
# #             cmd = "rm -rfv " + folder
# #             try:
# #                 os.system(cmd)
# #             except Exception as e:
# #                 print(e)
# #
# #         # Recreate the folder
# #         try:
# #             os.makedirs(folder)
# #         except Exception as e:
# #             print(e)
# #
# #     with open(args_file, 'w') as file_object:
# #         for trial in range(1, trials + 1):
# #             for n12 in n12s:
# #                 # id += 1
# #                 args_string = '--n12=%e --trial=%i\n' % (n12, trial)
# #                 file_object.write(args_string)
# #
# #     # Create lock file for the position file
# #     with open(position_lock, 'w'):
# #         pass
# #
# #     print("Argument list created. Launching sbatch...")
# #
# #     line_count = id
# # else:

print("Resuming simulation with the same arguments file")

# # Divide arguments into several files to avoid file access bottlenecks
# MAX_CONCURRENT = 500
# workers = jobs_count_tars_common * input_args.common + jobs_count_tars * input_args.dedicated
#
# # Count the number of arguments
# with open(args_file, 'r') as f:
#     arguments = [line.rstrip() for line in f]
# num_arguments = len(arguments)
#
# num_files_from_jobs = int(np.ceil(num_arguments / MAX_CONCURRENT))
# num_files = min([num_files_from_jobs, workers])
# arguments_per_file = int(np.ceil(num_arguments / num_files))
#
# # Split workers
# workers_dedicated = jobs_count_tars * input_args.dedicated
# split_workers_dedicated = [int(np.floor(workers_dedicated / num_files))] * num_files
# split_workers_dedicated[0] = workers
#
# print(f'Splitting the job into {num_files} files')

# for file_ind in range(num_files):
#     args_file = args_file_base + str(file_ind) + '.dat'

# # Copy lines
# start = file_ind * arguments_per_file
# end = min([(file_ind+1) * arguments_per_file, num_arguments])
# with open(args_file, 'w') as to_file:
#     to_file.writelines(arguments[start:end])

# Launch job managers
if script_name == 'job_manager.py':
    cmd_str = '{python_name} %s' % (script_name)
    popens = []
    pids = []
    for j in range(1, jobs_count + 1):
        cur_popen = subprocess.Popen([python_name, script_name + " " + args_file])
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
    # Launch on dedicated, tars
    if input_args.dedicated:
        cmd_str = f'sbatch --array=1-{jobs_count:d} {script_name} {args_file}'
        # print(cmd_str)
        # /dev/null
        os.system(cmd_str)
        print(f'Submitted {jobs_count} jobs to "dbc_pmo". Processing: {args_file}.')

    # Launch on common, tars
    if input_args.common:
        cmd_str = f'sbatch --array=1-{jobs_count_tars_common:d} {script_name_common} {args_file}'
        os.system(cmd_str)
        print(f'Submitted {jobs_count_tars_common} jobs to "common". Processing: {args_file}.')
