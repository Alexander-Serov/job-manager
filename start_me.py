# This function creates the argument list and launches job managers


import argparse  # for command-line arguments
import os  # for file operations
import socket  # for netowrk hostname
import subprocess  # for launching detached processes on a local PC
import sys  # to set exit codes
import shutil

import numpy as np

from constants import *

# Define arguments
arg_parser = argparse.ArgumentParser(
    description='Job manager. You must choose whether to resume simulations or restart and regenerate the arguments file')
arg_parser.add_argument('args_file', action='store', nargs='?', type=str, default='arguments.dat')
arg_parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + str(version))

# mode_group = arg_parser.add_mutually_exclusive_group(required=True)
# mode_group.add_argument('--restart', action='store_true')
# mode_group.add_argument('--resume', action='store_true')

arg_parser.add_argument('-c', '--common', dest='common', action='store_true')

# dedicated_group = arg_parser.add_mutually_exclusive_group()
# dedicated_group.add_argument('--dedicated', dest='dedicated', action='store_true')
arg_parser.add_argument('--nd', '--no-dedicated', dest='dedicated', action='store_false')
# arg_parser.add_argument('--delay', dest='delay', action='store', type=float, help='Delay between '
#                                                                                   'jobs in seconds')

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
# bl_restart = input_args.restart
args_file = input_args.args_file


def get_position_file(args_file):
    args_basename = os.path.splitext(os.path.basename(args_file))[0]
    position_file = args_basename + ".pos"
    return position_file


# if bl_restart:
#     # args_basename = os.path.splitext(os.path.basename(args_file))[0]
#     # position_file = args_basename + "_position.dat"
#     try:
#         os.unlink(get_position_file(args_file))
#     except Exception:
#         pass
#     print('Position file reset.')
# else:
#     print("Resuming from the last calculated position.")


if args_file == 'arguments.dat':
    # Find the next free arguments file name
    args_files_limit = 10000
    args_file_base, ext = os.path.splitext(os.path.basename(args_file))


    def get_args_file(i): return args_file_base + f'_{i:d}' + ext


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

# args_file_main = args_file
#
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


# # Divide arguments into several files to avoid file access bottlenecks
# MAX_CONCURRENT = 500

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

workers_count = jobs_count_tars_common * input_args.common + jobs_count_tars * input_args.dedicated

# Launch job managers
if script_name == 'job_manager.py':
    cmd_str = '{python_name} %s' % (script_name)
    popens = []
    pids = []
    for j in range(1, jobs_count + 1):
        cur_popen = subprocess.Popen([python_name, script_name + " " + new_args_file])
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
    shift = 0
    if input_args.dedicated:
        cmd_str = f'sbatch --array=1-{jobs_count:d} {script_name} {new_args_file} ' \
            f'{workers_count:d} {shift:d}'
        # print(cmd_str)
        # /dev/null
        os.system(cmd_str)
        print(f'Submitted {jobs_count} jobs to "dbc_pmo". Processing: {new_args_file}.')

    shift = int(jobs_count_tars * input_args.dedicated)
    # Launch on common, tars
    if input_args.common:
        cmd_str = f'sbatch --array=1-{jobs_count_tars_common:d} {script_name_common} ' \
            f'{new_args_file} {workers_count:d} {shift:d}'
        os.system(cmd_str)
        print(f'Submitted {jobs_count_tars_common} jobs to "common". Processing: {new_args_file}.')
