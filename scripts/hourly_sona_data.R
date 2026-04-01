#!/usr/bin/env Rscript
#SBATCH --job-name=sona_data
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --account=chem-cmde-2019
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cw1781@york.ac.uk
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sona_%j.out
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sona_%j.err


library(tidyverse)
library(purrr)
library(vroom)


# 1. Get the file list
sona_files = list.files("/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/sona_data", 
                        pattern = "Sonic_", 
                        recursive = TRUE, 
                        full.names = TRUE)

# 2. Define a function to process each file
process_hourly_sona <- function(file_path) {
  
  # Extract the datetime string from the filename (e.g., "20241201_1300")
  # This looks for 8 digits, an underscore, then 4 digits
  time_string <- str_extract(basename(file_path), "\\d{8}_\\d{4}")
  
  # Convert that string to a real POSIXct datetime object
  file_datetime <- ymd_hm(time_string)
  
  # Read the file and immediately summarize
  # We use read.table or read_csv depending on your file's delimiter
  dat <- read_csv(file_path) # Adjust if there are headers
  
  # Calculate mean of all numeric columns
  summary_row <- dat %>%
    summarise(across(where(is.numeric), \(x) mean(x, na.rm = TRUE))) %>%
    mutate(datetime = file_datetime) %>%
    relocate(datetime) # Put time at the start
  
  return(summary_row)
}

# 3. Map over all files and bind together
# This will be much lighter on your RAM!
all_hourly_data <- vroom(sona_files, ~{
  # Adding a print statement helps track progress for large datasets
  message("Processing: ", basename(.x))
  process_hourly_sona(.x)
})

# 4. Save the result
write_csv(all_hourly_data, "data/data/hourly_sona_summary.csv")
