#  creating concentration files 

# library
library(tidyverse)
library(arrow)
library(waclr)


data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/five_hz"
out_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/5Hz_input_files"
cal_root  <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data"


all_files <- list.files(data_root, full.names = TRUE, pattern = "\\.csv$", recursive = TRUE)

test_files <- all_files[24000:24001]

# processing function
args = commandArgs(trailingOnly = TRUE)[1]
i = as.numeric(args)+1


#for (i in seq_along(all_files)) {
  file_5hz <- all_files[100]
  message(sprintf("[%d/%d] Processing: %s", i, length(all_files), basename(file_5hz)))
  
  # --- read the hourly 5Hz CSV ---
  df_5hz <- read_csv(file_5hz) %>%
    mutate(datetime = parse_excel_date(TheTime, tz = "UTC")) %>%
    arrange(datetime) %>% 
    select(-c(CH1_sens, CH2_sens))
  
  if (nrow(df_5hz) == 0) next
  
  # --- determine calibration file(s) needed for this hour ---
  year_val  <- year(min(df_5hz$datetime))
  month_val <- sprintf("%02d", month(min(df_5hz$datetime)))
  cal_file  <- file.path(cal_root, year_val, paste0("param_", year_val, "_", month_val, ".parquet"))
  
  # --- read calibration parquet ---
  df_cal <- open_dataset(cal_file, format = "parquet") %>%
    collect() %>%
    mutate(sec = floor_date(datetime, "1 sec")) %>%
    distinct(sec, .keep_all = TRUE) 
  
  df_5hz <- df_5hz %>%
    rename_all(tolower) %>% 
    mutate(sec = floor_date(datetime, "1 sec")) %>%
    left_join(df_cal %>% select(sec, ch1_zero, ch2_zero, ch1_sens, ch2_sens, ce), by = "sec") %>% 
    mutate(
      ch1_hz = ifelse(ch1_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch1_hz),
      ch2_hz = ifelse(ch2_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch2_hz)) %>% 
    mutate(
      ch1_hz  = (ch1_hz - ch1_zero) / ch1_sens * 1e-3,
      ch2_hz = (((ch2_hz - ch2_zero) / ch2_sens * 1e-3) - ch1_hz) / ce)  
    mutate(unixTime = as.numeric(datetime), 
           veloXaxs = u, 
           veloYaxs = vv, 
           veloZaxs = w, 
           tempAir = temp_sonic, 
           presAtm = reading_data,
           relative_humidity = "reading_data",
           distZaxsAbl = "abl", 
           distZaxsMeas = "meas", 
           rtioMoleDryH2o = eddy4R::rtio.mole.h2o.temp.pres.rh()) %>% 
  select(c(unixTime, veloXaxs, veloYaxs, veloZaxs, tempAir, presAtm, distZaxsAbl, distZaxsMeas, rtioMoleDryH2o, 
           ch1_hz, ch2_hz))
  
  # --- save in same structure as input ---
  out_file <- file.path(out_root, 
                        year_val, month_val, basename(file_5hz))
  
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
  
  write_csv(df_5hz, out_file)
#}









