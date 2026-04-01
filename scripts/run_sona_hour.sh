#!/bin/bash
#SBATCH --job-name=sona_array
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G                      # Each file only needs ~4GB
#SBATCH --time=00:30:00              
#SBATCH --account=chem-cmde-2019
#SBATCH --array=1-1000%50             # Processes 1000 files, 50 at a time
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sona/sona_%a.out
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sona/sona_%a.err

# Load R module if required
 module load R/4.4.1-gfbf-2023b

# Run the R script, passing the array ID
Rscript hourly_sona_array.R $SLURM_ARRAY_TASK_ID