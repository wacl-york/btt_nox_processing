#!/usr/bin/env Rscript
#SBATCH --job-name=cal_1Hz
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --account=chem-cmde-2019
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cw1781@york.ac.uk
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sona_hourly_2024.out
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sona_hourly_2024%j.err

# Run this once the Slurm Array is 'COMPLETED'
library(tidyverse)

final_2024 <- list.files("/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/data/data/temp_sona/2024", 
                         full.names = TRUE) %>%
  map_df(read_csv) %>%
  arrange(datetime) # This puts them in the correct time order!

write_csv(final_2024, "/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/data/data/sona_hourly_2024.csv")
