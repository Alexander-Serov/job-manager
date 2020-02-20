"""
The function reads one line of arguments from a file and launches code execution.
It uses FileLock to correctly handle the co-existence of several job_managers in parallel.

The script must be launched with two arguments:
1. Name of the arguments file
2. Job_id (optional)

isort:skip_file
"""

import os  # for shell execution of the code
import sys
from filelock import FileLock, Timeout  # for file locks
from constants import lock_timeout, stop_file
import warnings
import time
import traceback

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from calculate import simulate_and_calculate_Bayes_factor_terminal as main  # actual calculations
from stopwatch import stopwatch

EXIT_SUCCESS = 0
EXIT_INTERRUPTED = 'Program interrupted per user request'
EXIT_TIMEOUT = 0  # 'Timeout waiting for lock on the position file'
EXIT_PARENT_UNEXPECTED = 'Unexpected parent exit. This line should not have been reached.'

time_start = time.time()

# Get arguments file name
if len(sys.argv) > 1:
    args_file = sys.argv[1]
    args_basename = os.path.splitext(os.path.basename(args_file))[0]
    position_file = args_basename + ".pos"
    print(f'Processing arguments from "{args_file}". Position tracking file: "{position_file}".')
else:
    raise RuntimeError('You must provide the name of the arguments file to the "job_manager.py" '
                       'script')
position_lock = os.path.splitext(os.path.basename(position_file))[0] + '.lock'

# Get job_id to allow stopping
if len(sys.argv) > 2:
    job_id = sys.argv[2]
    stop_file = f'{stop_file}_{job_id}'
else:
    stop_file = '<none>'
print(f'To stop these calculations create a file "{stop_file}" in the job-manager\'s folder')

if len(sys.argv) > 3:
    workers_count = int(sys.argv[3])
else:
    raise RuntimeError('You must provide the total workers count as the 3rd argument')

# if len(sys.argv) > 4:
#     shift = sys.argv[4]
# else:
#     raise RuntimeError('You must provide the initial id shift as the 4th argument')

if len(sys.argv) > 4:
    task_id = int(sys.argv[4]) - 1
else:
    raise RuntimeError('You must provide the job id as the 5th argument')


# Check stop signal
def check_stop():
    if os.path.exists(stop_file):
        print('\nReceived a stop signal. Terminating.')
        sys.exit(EXIT_INTERRUPTED)


check_stop()

lock = FileLock(position_lock, timeout=lock_timeout)
# Make sure the position file exists
try:
    with open(position_file, 'r'):
        pass
except FileNotFoundError:
    with open(position_file, 'w') as f:
        f.write('{0:d}'.format(0))

# Count the number of lines in the arguments file
with open(args_file, 'r') as f:
    for i, l in enumerate(f):
        pass
max_lines = i + 1


def get_next_line():
    """
    Reads the value from the position file and increases it by 1 if it does not exceed the number of
    lines in the argument file.
    """
    # try:
    #     with stopwatch('Waiting for the lock'):
    #         with lock:
    #             with open(position_file, 'r') as fp_position:
    #                 next_i = int(fp_position.read())
    #             if next_i < max_lines:
    #                 # Update the line number, but only if it is smaller than the file length
    #                 with open(position_file, 'w') as fp_position:
    #                     fp_position.write('{0:d}'.format(next_i + 1))
    #
    # except Timeout as e:
    #     print(f"Reached timeout ({lock_timeout} s) while waiting for the file lock.")
    #     sys.exit(EXIT_TIMEOUT)
    next_i = get_next_line.pos * workers_count + task_id
    get_next_line.pos += 1
    return next_i

print(task_id, workers_count)

get_next_line.pos = 0
print('This instance will process the following strings fromt the arguments file:\n',
      list(range(task_id, max_lines + 1, workers_count)))


# Read the next in queue line in the arguments file
def read_and_calculate():
    with open(args_file, 'r') as file_object:
        next_i = get_next_line()
        if next_i < max_lines:
            bl_first = 1
            for i, line in enumerate(file_object):
                if i == next_i:
                    if bl_first:
                        delay = time.time() - time_start
                        print(f'First calculation started {round(delay, 1)}s after launch')
                        bl_first = 0
                    # Get current arguments and simulate
                    cur_args = line.strip()
                    print(
                        f"\nLaunching calculation line #{next_i} with parameters '" + cur_args + "'")

                    try:
                        main(cur_args)
                    except Exception as e:
                        print(f"Encountered unhandled exception while calculating line #{next_i} "
                              f"with parameters '" + cur_args + "'.")
                        print('Exception:\n', e)
                        traceback.print_exc()
                        # print('Skipping.')
                        # raise(e)

                    check_stop()
                    next_i = get_next_line()

                    # If the next_i is smaller than the current, it is likely the calculation has
                    # been reset. Need to restart from the start
                    if next_i < i:
                        print('Detected arguments file change. Reloading the arguments file')
                        read_and_calculate()
                        warnings.warn('Unexpected parent exit.')
                        sys.exit(EXIT_PARENT_UNEXPECTED)
        else:
            print(f'Position file points to the end of the arguments file ({next_i} >= '
                  f'{max_lines}). No calculations have been performed.')

    print("Queue empty. Finishing.")
    sys.exit(EXIT_SUCCESS)


read_and_calculate()
# print("Arguments file empty. Finishing.")
# sys.exit(EXIT_SUCCESS)
