#!/usr/bin/env bash
#SBATCH --job-name=make_ec_inputs
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=10G
#SBATCH --time=0-00:10:00
#SBATCH --account=chem-cmde-2019
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cw1781@york.ac.uk
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/make_ec_inputs2_2022/%x_%j_%a.log
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/make_ec_inputs2_2022/%x_%j_%a.err
#SBATCH --array=0-8266
set -e

module purge
module load Apptainer/latest

apptainer exec --bind /mnt/scratch/projects/chem-cmde-2019/btt_processing/:/data/,/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/:/scripts/ /mnt/longship/projects/chem-cmde-2019/eddy4r/eddy4r.york_dev Rscript /scripts/final_pipeline/make_ec_inputs_3_2.R $SLURM_ARRAY_TASK_ID 2022
