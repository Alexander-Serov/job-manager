"""
The function reads one line of arguments from a file and launches code execution.
It uses FileLock to correctly handle the co-existence of several job_managers in parallel.

isort:skip_file
"""

import os  # for shell execution of the code
import sys
from filelock import FileLock, Timeout  # for file locks
from constants import args_file, args_lock, lock_timeout, position_file, position_lock, stop_file
import warnings

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from calculate import simulate_and_calculate_Bayes_factor_terminal as main  # actual calculations

EXIT_SUCCESS = 0
EXIT_INTERRUPTED = 'Program interrupted per user request'
EXIT_TIMEOUT = 'Timeout waiting for lock on the position file'
EXIT_PARENT_UNEXPECTED = 'Unexpected parent exit. This line should not have been reached.'

if len(sys.argv) > 1:
    job_id = sys.argv[1]
    stop_file = f'{stop_file}_{job_id}'
print(f'To stop these calculations create a file "{stop_file}" in the job-manager\'s folder')


# Check stop signal
def check_stop():
    if os.path.exists(stop_file):
        print('\nReceived a stop signal. Terminating.')
        sys.exit(EXIT_INTERRUPTED)


check_stop()

lock = FileLock(position_lock, timeout=lock_timeout)
if not os.path.exists(position_file):
    with open(position_file, 'w') as fp_position:
        fp_position.write('{0:d}'.format(0))

# Count the number of lines in the arguments file
with open(args_file, 'r') as f:
    for i, l in enumerate(f):
        pass
max_lines = i + 1


def get_next_line():
    try:
        with lock:
            with open(position_file, 'r') as fp_position:
                next_i = int(fp_position.read())
            if next_i < max_lines:
                # Update the line number, but only if it is smaller than the file length
                with open(position_file, 'w') as fp_position:
                    fp_position.write('{0:d}'.format(next_i + 1))

    except Timeout as e:
        print(f"Reached timeout ({lock_timeout} s) while waiting for the file lock.")
        sys.exit(EXIT_TIMEOUT)
    return next_i


# Read the next in queue line in the arguments file
def read_and_calculate():
    with open(args_file, 'r') as file_object:
        next_i = get_next_line()
        if next_i < max_lines:
            for i, line in enumerate(file_object):
                if i == next_i:
                    # Get current arguments and simulate
                    cur_args = line.strip()
                    print(
                        f"\nLaunching calculation line #{next_i} with parameters '" + cur_args + "'")
                    main(cur_args)
                    check_stop()
                    next_i = get_next_line()

                    # If the next_i is smaller than the current, it means that the calculation was
                    # reset. Need to restart from the start
                    if next_i < i:
                        print('Detected arguments file change. Reloading the arguments file')
                        read_and_calculate()
                        warnings.warn('Unexpected parent exit.')
                        sys.exit(EXIT_PARENT_UNEXPECTED)

    print("Arguments file empty. Finishing.")
    sys.exit(EXIT_SUCCESS)


read_and_calculate()
# print("Arguments file empty. Finishing.")
# sys.exit(EXIT_SUCCESS)
