#!/usr/bin/env bash
#SBATCH --job-name=flux
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=10G
#SBATCH --time=0-00:30:00
#SBATCH --account=chem-cmde-2019
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cw1781@york.ac.uk
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/flux_v2/%x_%A_%a.log
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/flux_v2/%x_%A_%a.err
set -e

# usage: sbatch --job-name=flux_<mode>_<year> --array=... get_flux.sbatch <year> <unconstrained|constrained>
YEAR=$1
MODE=$2

# outputs (fluxes, ACF, lag ranges) go to chem-bttao-2026 scratch; the new inputs are read from longship (read only)
OUT_HOST=/mnt/scratch/projects/chem-bttao-2026/btt_processing/processing        # = /processing in the container
IN_HOST=/mnt/longship/projects/chem-bttao-2026/btt_processing/processing/ec_2/in   # = /processing/ec_2/in in the container
LIST=/mnt/scratch/users/cw1781/btt_cal_processing/file_lists/ec_in_${YEAR}.txt

# file for this task: line (task ID + 1) of the year's sorted list, converted to the container path
FILE_HOST=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$LIST")
if [ -z "$FILE_HOST" ]; then echo "No file for task $SLURM_ARRAY_TASK_ID in $LIST"; exit 1; fi
FILE_SELECT="/processing/ec_2/in${FILE_HOST#$IN_HOST}"
echo "Task $SLURM_ARRAY_TASK_ID: $FILE_SELECT ($MODE)"

mkdir -p ${OUT_HOST}/ec_2/in       # empty mount point for the inputs (nothing is written here)

module purge
module load Apptainer/latest

apptainer exec --env FILE_SELECT=${FILE_SELECT},LAG_MODE=${MODE} --bind ${OUT_HOST}/:/processing/,${IN_HOST}/:/processing/ec_2/in/,/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/:/scripts/ /mnt/longship/projects/chem-cmde-2019/eddy4r/eddy4r.york_dev Rscript /scripts/final_pipeline/flux_run.R