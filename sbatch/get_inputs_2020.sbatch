#!/usr/bin/env bash
#SBATCH --job-name=make_ec_inputs # Job name
#SBATCH --ntasks=1                      # Number of MPI tasks to request
#SBATCH --cpus-per-task=1               # Number of CPU cores per MPI task
#SBATCH --mem=10G                      # Total memory to request
#SBATCH --time=0-00:10:00               # Time limit (DD-HH:MM:SS)
#SBATCH --account=chem-cmde-2019        # Project account to use
#SBATCH --mail-type=END,FAIL            # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=cw1781@york.ac.uk   # Where to send mail
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/logs/make_ec_inputs_2020/%x_%j_%a.log       # Standard output log
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/logs/make_ec_inputs_2020/%x_%j_%a.err        # Standard error log
#SBATCH --array=0-2830          # Array range
# Abort if any command fails
set -e

# purge any existing modules
module purge

# Load modules
module load Apptainer/latest

# Commands to run
apptainer exec --bind /mnt/scratch/projects/chem-cmde-2019/btt_processing/:/data/,/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/:/scripts/ /mnt/longship/projects/chem-cmde-2019/eddy4r/eddy4r.york_dev Rscript /scripts/get_flux_input_2020.R $SLURM_ARRAY_TASK_ID
