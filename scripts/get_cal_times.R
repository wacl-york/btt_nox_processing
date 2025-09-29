# library
library(tidyverse)
library(waclr)
library(vroom)
library(arrow)


param_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params"

param_files <- list.files(file.path(data_root),
                          full.names = TRUE,
                          pattern = "param.*\\.parquet",
                          recursive = TRUE) 


# Function to extract calibration times from one parquet file
get_cal_times <- function(file) {
  open_dataset(file, format = "parquet") %>%
    collect() %>%  # read into R
    mutate(date = as_datetime(unix_time, tz = "UTC")) %>%
    filter(no_valve == 1) %>%
    filter(no_cal == 1) |> 
    mutate(cal_hour = floor_date(date, "hour")) %>%
    group_by(cal_hour) %>%
    summarise(first_cal = min(date, na.rm = TRUE), .groups = "drop")
}

cal_times_all <- map_dfr(param_files, get_cal_times) %>%
  arrange(first_cal) |> 
  rename("date" = cal_hour) %>% 
  select(date)

write_csv(cal_times_all, "data/data/cal_times_all.csv")
