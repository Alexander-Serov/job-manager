#! /bin/bash

# Sbatch options
#SBATCH -J out-of
#SBATCH --qos=normal
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=300MB
#SBATCH --mail-type=END, --mail-user=alexander.serov@pasteur.fr




# Constants
logs_folder="./logs/"
# args_file="arguments.dat"

# # Read command line arguments from file
# argument=`awk "NR==${SLURM_ARRAY_TASK_ID}" $args_file`
ARGS_FILE=$1
WORKERS_COUNT=$2
SHIFT=$3

# Launch srun with these argument sequence
module load Python/3.7.2
srun -o "${logs_folder}log_job_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out" -e "${logs_folder}log_job_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out" -J "${SLURM_ARRAY_TASK_ID}" python3 job_manager.py $ARGS_FILE $SLURM_ARRAY_JOB_ID $WORKERS_COUNT $((SHIFT + SLURM_ARRAY_TASK_ID))



