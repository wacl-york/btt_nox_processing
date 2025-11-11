#  creating concentration files 

# library
library(tidyverse)
library(arrow)
library(waclr)

# extra data needed 

# issue is that the parquet files were created wrong

data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/five_hz"
all_files <- list.files(data_root, full.names = TRUE, pattern = "\\.parquet$", recursive = TRUE)

all_files <- all_files[1:1]

# processing function

process_5hz <- function(file_5hz, cal_root = "filepath/cal_data", 
                        out_root = "filepath/out") {
  message("Processing 5Hz file: ", file_5hz)
  
  # --- read 5Hz raw data
  df_5hz <- open_dataset(file_5hz, format = "parquet") %>%
    collect() %>%
    mutate(datetime = parse_excel_date(TheTime, tz = "UTC")) %>%
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
      ch1_hz  = (ch1_hz - ch1_zero) / ch1_sens * 1e-3,
      ch2_hz = (((ch2_hz - ch2_zero) / ch2_sens * 1e-3) - no) / ce
    ) %>% 
    mutate(unixTime = as.POSIXct(datetime, format), 
           veloXaxs = "x", 
           veloYaxs = "y", 
           veloZaxs = "z", 
           tempAir = "temp", 
           presAtm = rnorm(nrow(), 101325, 1000), 
           distZaxsAbl = "abl", 
           distZaxsMeas = "meas", 
           rtioMoleDryH2o = rnorm(nrow(), 50, 20)) %>% 
    mutate(hour_block = floor_date(datetime, "hour"))
  
  hourly_groups <- df_joined %>% 
    group_split(hour_block)
  
  walk(hourly_groups, function(df_hour){
    this_hour <- unique(df_hour$hour_block)
    year_val <- format(this_hour, "%Y")
    month_val <- format(this_hour, "%m")
    
    year_month_dir <- file.path(out_root, year_val, month_val)
    dir.create(year_month_dir, recursive = TRUE, showWarnings = FALSE)
    
    hour_str <- format(this_hour, "%Y_%m_%d_%H")
    out_file <- file.path(year_month_dir, paste0("concentration_5hz_", hour_str, ".csv"))
    
    if (!file.exists(out_file)) {
      write_csv(df_hour, out_file)
      message("Saved hourly CSV: ", out_file)
    } else {
      message("Skipping existing file: ", out_file)
    }
  })
  
  invisible(NULL)
}
  
for (i in seq_along(all_files)) {
  file_5hz <- all_files[i]
  
  message(sprintf("[%d/%d] Processing: %s", i, length(all_files), basename(file_5hz)))
  
  # --- call your processing function ---
  process_5hz(
    file_5hz,
    cal_root = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data",
    out_root = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/5Hz_input_files"
  )
}





