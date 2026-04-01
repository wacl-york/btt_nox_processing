#!/usr/bin/env Rscript
library(tidyverse)
library(vroom)
library(lubridate)

# --- 1. GET THE FILE LIST ---
sona_files <- list.files("/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/sona_data/2024", 
                         pattern = "Sonic_", recursive = TRUE, full.names = TRUE) %>% sort()

# --- 2. GET THIS TASK'S FILE ---
args <- commandArgs(trailingOnly = TRUE)
task_id <- as.numeric(args[1])

if (task_id > length(sona_files)) stop("Task ID exceeds file list length")
file_path <- sona_files[task_id]

# --- 3. PROCESS ---
time_string <- str_extract(basename(file_path), "\\d{8}_\\d{4}")
file_datetime <- ymd_hm(time_string)

# Read and average
dat <- vroom(file_path, show_col_types = FALSE, progress = FALSE)
summary_row <- dat %>%
  summarise(across(where(is.numeric), ~mean(.x, na.rm = TRUE))) %>%
  mutate(datetime = file_datetime, filename = basename(file_path)) %>%
  relocate(datetime)

# --- 4. SAVE TO TEMP DIR ---
# We save individual files to a temp folder, then combine them later
temp_dir <- "/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/data/data/temp_sona/2024"
if(!dir.exists(temp_dir)) dir.create(temp_dir, recursive = TRUE)

out_name <- paste0("idx_", task_id, ".csv")
write_csv(summary_row, file.path(temp_dir, out_name))

message("Processed task ", task_id, ": ", basename(file_path))