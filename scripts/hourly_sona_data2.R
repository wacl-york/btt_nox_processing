#!/usr/bin/env Rscript
#SBATCH --job-name=sona_data
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --account=chem-cmde-2019
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cw1781@york.ac.uk
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sona2_%j.out
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sona2_%j.err

library(tidyverse)
library(vroom)
library(lubridate)


# --- 1. GET FILE LIST ---
sona_files <- list.files("/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/sona_data", 
                         pattern = "Sonic_", 
                         recursive = TRUE, 
                         full.names = TRUE)

#file_path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/sona_data/2020/Sonic_Licor_BT_20200717_0900.sona"

# --- 2. DEFINE PROCESSING FUNCTION ---
process_hourly_sona <- function(file_path) {
  # Extract datetime from filename
  time_string <- str_extract(basename(file_path), "\\d{8}_\\d{4}")
  file_datetime <- ymd_hm(time_string)
  
  # Use vroom for faster reading than read_csv
  # progress = FALSE keeps your .out log file from getting huge
  dat <- vroom(file_path, show_col_types = FALSE, progress = FALSE)
  
  # Summarize numeric columns
  summary_row <- dat %>%
    summarise(across(where(is.numeric), ~mean(.x, na.rm = TRUE))) %>%
    mutate(datetime = file_datetime) %>%
    relocate(datetime)
  
  return(summary_row)
}

# --- 3. BATCH BY YEAR ---
# We organize files by year to save checkpoints
sona_info <- data.frame(path = sona_files) %>%
  mutate(filename = basename(path),
         # Looks for 4 digits that follow an underscore
         year = str_extract(filename, "(?<=_)\\d{4}"))

unique_years <- sort(unique(sona_info$year))

# --- 4. SEQUENTIAL LOOP ---
for (yr in unique_years) {
  message("--- Starting Year: ", yr, " ---")
  
  year_files <- sona_info %>% filter(year == yr) %>% pull(path)
  
  # Process files for this year one by one
  year_data <- map_df(year_files, function(f) {
    # Optional: print filename to log every 50 files so you can see it's alive
    # if (which(year_files == f) %% 50 == 0) message("Processing: ", basename(f))
    process_hourly_sona(f)
  })
  
  # Save the individual year file
  output_path <- paste0("/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/data/data/", yr, ".csv")
  write_csv(year_data, output_path)
  
  message("--- Completed Year: ", yr, " (", nrow(year_data), " hours summarized) ---")
}

message("All years processed successfully.")