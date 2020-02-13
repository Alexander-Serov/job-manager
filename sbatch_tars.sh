#! /bin/bash

# Sbatch options
#SBATCH -J out-of
#SBATCH -p dbc_pmo
#SBATCH --qos=dbc
##### SBATCH --qos=fast
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=300MB
#SBATCH --mail-type=END, --mail-user=alexander.serov@pasteur.fr




# Constants
logs_folder="./logs/"
# args_file="arguments.dat"

# # Read command line arguments from file
# argument=`awk "NR==${SLURM_ARRAY_TASK_ID}" $args_file`

# Launch srun with these argument sequence
module load Python/3.7.2
echo $argument
srun -o "${logs_folder}log_job_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out" -e "${logs_folder}log_job_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out" -J "${SLURM_ARRAY_TASK_ID}" python3 job_manager.py $1 $SLURM_ARRAY_JOB_ID



