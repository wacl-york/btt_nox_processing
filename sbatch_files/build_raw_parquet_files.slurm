#!/bin/bash
#SBATCH --job-name="btt_build_parquet"
#SBATCH --time=00:05:00
#SBATCH --mem=1G
#SBATCH --account=ncas_obs
#SBATCH --partition=standard
#SBATCH --qos=standard
#SBATCH -o %j.out
#SBATCH -e %j.err

singularity exec btt_nox_processing_dev.sif Rscript /btt_nox_processing/scripts/build_raw_parquet_files_array_task.R $SLURM_ARRAY_TASK_ID