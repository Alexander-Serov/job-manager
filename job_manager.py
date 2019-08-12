"""
The function reads one line of arguments from a file and launches code execution.
It uses FileLock to correctly handle the co-existence of several job_managers in parallel.

isort:skip_file
"""


import os  # for shell execution of the code
import socket  # for netowrk hostname
import sys

from constants import args_file, args_lock, lock_timeout
from filelock import FileLock, Timeout  # for file locks

# Add parent folder to path and import the actual calculations code
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from simLattice import simulate_command_line

# from signal import signal, SIGPIPE, SIG_DFL		# To handle broken pipe messages

# calculation_script = "main.py"

# # Set Python to ignore absent write pipe for `print` operator
# signal(SIGPIPE,SIG_DFL)

this_script_path = os.path.dirname(os.path.abspath(__file__))

while True:
    # Try to obtain lock
    lock = FileLock(args_lock, timeout=lock_timeout)
    try:
        # If get lock
        with lock:
            # Read and store all lines in the arguments file
            with open(args_file, 'r') as file_object:
                all_lines = file_object.read().splitlines(True)

            # If arguments file is empty, end program
            if len(all_lines) == 0:
                print("Arguments file empty. Finishing.")
                sys.exit(0)

            # Write all lines back except for the first one
            with open(args_file, 'w') as file_object:
                file_object.writelines(all_lines[1:])

            # Get current arguments and clen
            cur_args = all_lines[0].strip()
            del all_lines

    # If unable to get lock
    except Exception as e:
        print("Ecountered unknown exception while getting the file lock: '" + e + "'")
        sys.exit(-1)

    # Launch calculation with current arguments
    print("Launching calculations with parameters '" + cur_args + "'")
    simulate_command_line(cur_args)
