
library(arrow)
library(tidyverse)

cal_file <- 

data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params"

cal_file <- read_csv("data/data/cal_file_all.csv")
cal_times <- cal_file$date

param_files <- list.files(file.path(data_root),
                          full.names = TRUE,
                          pattern = "param.*\\.parquet",
                          recursive = TRUE)

# Extract year-month to match cal times
param_months <- param_files %>%
  basename() %>%
  stringr::str_extract("\\d{4}_\\d{2}") %>%
  stringr::str_replace("_", "-") %>%
  ym()  # first day of month

params <- open_dataset(data_root, format = "parquet")




