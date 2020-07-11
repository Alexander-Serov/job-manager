

import numpy as np
from numpy import exp, log, log10

# Constants
version = 20180411

# %% Parameters for out-of-equilibrium Bayes factor calcualtions
trials = 5
n12_range = [1e3, 1e4]
n12_points = 10
n12s = np.logspace(log10(n12_range[0]), log10(n12_range[1]), num=n12_points)

jobs_count_tars = 204  # max: 224
jobs_count_tars_common = 3000  # 1000
jobs_count_t_bayes = 132
jobs_count_onsager = 10
manager_script = "job_manager.py"
DETACHED_PROCESS = 0x00000008


dim = 2  # number of dimensions (1 or 2)
L = 1.0  # linear size of the system, in um
x_min = 0.0  # in um
x_max = L  # in um
t_step = 0.04  # in seconds


# Diffusivity constants
D_0 = 0.01  # in um^2/s
q = 1  # number of D peaks
gamma_drag = 400.0  # viscous drag, in fN * s / um


# Total force constants
# >> saw-tooth case for main check <<
# D_case = 1
# D_ratio = 2.0	# Ratio between max and min of the diffusivity
# ksi_range = (-25.0, 25.0)	# range of ratios of the total force to diffusivity gradient
# ksi_step = 0.5	# sampling step in the interval
# trials = 1000	# 1000
# N = int(1.0e4) # int(1.0e6) or int(1.0e5)

# >> round-well case for gradient jump check <<
D_case = 2
D_ratio = 10.0  # Ratio between max and min of the diffusivity
well_radius = L / 6.0
ksi_range = (0.5, 25.0)  # range of ratios of the total force to diffusivity gradient
ksi_step = 100.0  # sampling step in the interval

N = int(1.0e5)  # int(1.0e6) or int(1.0e5)


# Simulation parameters
progress_update_interval = 100.0
# Integer. How many intermediate smaller steps are made before the next point is saved
internal_steps_number = 100
k = 2.0 * q / L * (D_ratio - 1.0)  # D'/D
D_grad_abs = k * D_0  # absolute value of the diffusivity gradient used for force calculations

# Batch parameters

sleep_time = 0.2
logs_folder = "./logs/"
output_folder = "./output/"
args_file = "arguments.dat"
args_lock = "arguments.lock"
position_file = 'position.dat'
position_lock = 'position.lock'
stop_file = 'stop'
lock_timeout = 600	# s


output_folder = './output/'
CSV_DELIMITER = ';'


# k = 100.0 # diffusivity gradient, D'/D, in um^-1

max_D_case = 7

max_f_case = 8

str_mode = 'periodic'  # bc_type = ENUM_BC_PERIODIC;
