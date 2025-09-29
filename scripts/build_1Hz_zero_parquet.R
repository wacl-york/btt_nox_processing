# getting 1 Hz zero data

# library
library(tidyverse)
library(arrow)
library(future)
library(purrr)
library(furrr)


process_month_zeros <- function(f, out_dir = "filepath/zeros/") {
  message("Processing zeros for: ", f)
  
  # Read parquet for one month
  df <- open_dataset(f, format = "parquet") %>%
    collect() %>%
    mutate(datetime = as_datetime(timestamp, tz = "UTC")) %>%
    arrange(datetime)
  
  if (nrow(df) == 0) return(NULL)
  
  # --- extract zero calibration points
  zero_data <- df %>%
    filter(zero_valve_1 == 1.0, zero_valve_2 == 1.0) %>%
    mutate(second = second(datetime)) %>%
    filter(between(second, 5, 15)) %>%
    select(date = datetime, CH1_Hz, CH2_Hz) %>%
    arrange(date)
  
  if (nrow(zero_data) == 0) {
    warning("No zeros found in file: ", f)
    return(NULL)
  }
  
  # --- linear interpolation of zeros at 1Hz
  df <- df %>%
    mutate(
      ch1_zero = approx(zero_data$date, zero_data$CH1_Hz, xout = datetime, rule = 2)$y,
      ch2_zero = approx(zero_data$date, zero_data$CH2_Hz, xout = datetime, rule = 2)$y
    )
  
  # --- figure out year for subfolder
  year_val <- year(min(df$datetime, na.rm = TRUE))
  year_dir <- file.path(out_dir, as.character(year_val))
  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
  
  # --- write parquet
  out_file <- file.path(year_dir, basename(f))
  write_parquet(df, out_file)
  
  return(out_file)
}



data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params"

param_files <- list.files(file.path(data_root),
                          full.names = T, 
                          pattern = "\\.parquet$",
                          recursive = T)


plan(multisession, workers = 4)

future_walk(param_files, process_month_zeros)


