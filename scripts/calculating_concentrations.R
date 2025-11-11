#  creating concentration files 

# library
library(tidyverse)
library(arrow)


# processing function

process_5hz <- function(file_5hz, cal_root = "filepath/calibrated/", out_root = "filepath/five_hz_concentrations/") {
  message("Processing 5Hz file: ", file_5hz)
  
  # --- read 5Hz raw data
  df_5hz <- open_dataset(file_5hz, format = "parquet") %>%
    collect() %>%
    mutate(datetime = as_datetime(timestamp, tz = "UTC")) %>%
    arrange(datetime)
  
  if (nrow(df_5hz) == 0) return(NULL)
  
  # --- clean raw Hz signals
  df_5hz <- df_5hz %>%
    mutate(
      ch1_hz = ifelse(ch1_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch1_hz),
      ch2_hz = ifelse(ch2_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch2_hz),
      sec = floor_date(datetime, "1 sec")
    )
  
  # --- figure out which calibration parquet(s) are needed
  year_vals <- unique(year(df_5hz$datetime))
  cal_files <- map(year_vals, function(y) {
    file.path(cal_root, as.character(y), 
              gsub("NOx_five_hz_", "NOx_params_", basename(file_5hz)))
  })
  
  # --- read calibration data
  df_1hz <- map_dfr(cal_files, ~open_dataset(.x, format = "parquet") %>%
                      collect()) %>%
    mutate(sec = floor_date(datetime, "1 sec")) %>%
    distinct(sec, .keep_all = TRUE) 
  
  # --- join calibration to 5Hz
  df_joined <- df_5hz %>%
    left_join(df_1hz %>% 
                select(sec, ch1_zero, ch2_zero, ch1_sens, ch2_sens, ce),
              by = "sec")
  
  # --- calculate concentrations
  df_joined <- df_joined %>%
    mutate(
      no  = (ch1_hz - ch1_zero) / ch1_sens * 1e-3,
      no2 = (((ch2_hz - ch2_zero) / ch2_sens * 1e-3) - no) / ce
    )
  
  # --- save to yearly subfolder
  year_val <- year(min(df_joined$datetime, na.rm = TRUE))
  year_dir <- file.path(out_root, as.character(year_val))
  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
  
  out_file <- file.path(year_dir, gsub("NOx_five_hz_", "NOx_five_hz_concentrations_", basename(file_5hz)))
  write_parquet(df_joined, out_file)
  
  return(out_file)
}





