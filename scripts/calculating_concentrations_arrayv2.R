#  creating concentration files 

# library
library(tidyverse)
library(arrow)
library(data.table)


# waclr dependency ####

parse_unix_time <- function(x, tz = "UTC", origin = "1970-01-01") {
  
  # A switch for my common usage
  if (tz == "nz") tz <- "Etc/GMT-12"
  
  # Parse
  x <- as.POSIXct(x, tz = tz, origin = origin)
  
  return(x)
  
}

parse_excel_date <- function (x, tz = "UTC", type = "windows") 
{
  type <- stringr::str_to_lower(type)
  type <- stringr::str_replace_all(type, "\\.| ", "_")
  if (!type %in% c("windows", "os_x_2007")) {
    stop("Type must be 'windows' or 'os_x_2007'", call. = FALSE)
  }
  if (!class(x) == "numeric") 
    x <- as.numeric(x)
  if (type == "windows") 
    x <- (x - 25569) * 86400
  if (type == "os_x_2007") 
    x <- (x - 24107) * 86400
  x <- parse_unix_time(x, tz = tz)
  return(x)
}



# data roots ####

data_root <- "/data/raw_data/five_hz/2025"
out_root <- "/data/processing/5Hz_input_files"
met_root <- "/data/processing/met_data_formatted"
cal_root  <- "/data/processing/1Hz_cal_data"

print("getting file list")

all_files = system(paste("find", data_root, " -type f -name '*.csv'"), intern = TRUE) %>% 
  sort()

# processing function ####
args = commandArgs(trailingOnly = TRUE)[1]
i = as.numeric(args)+1

file_5hz <- all_files[i]

base_5hz = basename(file_5hz)

fileDate = as.POSIXct(base_5hz, format = "NOx_5Hz_%y_%m_%d_%H%M%S.csv", tz = "UTC")

if(fileDate > ymd_hms("2025-01-01 00:00:00")){
  
  message(sprintf("[%d/%d] Processing: %s", i, length(all_files), basename(file_5hz)))
  
  # --- read the hourly 5Hz CSV ---
  df_5hz <- read_csv(file_5hz) %>% 
    mutate(datetime = parse_excel_date(TheTime)) %>% 
    ungroup() %>% 
    arrange(datetime) %>% 
    select(-c(CH1_sens, CH2_sens))
  
  if (nrow(df_5hz) == 0) next
  
  # --- determine calibration file(s) needed for this hour ---
  year_val  <- year(min(df_5hz$datetime))
  month_val <- sprintf("%02d", month(min(df_5hz$datetime)))
  day_val   <- sprintf("%02d", day(min(df_5hz$datetime)))
  hour_val  <- sprintf("%02d", hour(min(df_5hz$datetime)))  
  
  cal_file  <- file.path(cal_root, year_val, paste0("param_", year_val, "_", month_val, ".parquet"))
  
  # ----------------------------------------------------------
  # READ CALIBRATION FILE
  # ----------------------------------------------------------
  
  df_cal <- open_dataset(cal_file, format = "parquet") %>%
    collect() %>%
    mutate(sec = floor_date(datetime, "1 sec")) %>%
    distinct(sec, .keep_all = TRUE) 
  
  # ----------------------------------------------------------
  # READ MATCHING READING DATA FILE
  # ----------------------------------------------------------
  reading_folder <- file.path(met_root, year_val, month_val)
  
  yy <- as.integer(year_val) %% 100
  dd <- as.integer(day_val)
  mm <- as.integer(month_val)
  wxt_filename <- sprintf("WXT_%02d_%02d_%02d.dat", yy, mm, dd)
  reading_file <- file.path(reading_folder, wxt_filename)
  
  if (length(reading_candidates) > 0) {
    reading_file <- reading_candidates[1]
    message(sprintf("Reading file found: %s", basename(reading_file)))
    
    # Read in
    df_reading <- read_csv(reading_file)
    
    df_reading <- df_reading %>%
          mutate(
            sec = floor_date(datetime, "1 sec"),
            # extract met variables (temporary assumption)
            presAtm = X11,
            relative_humidity = X7
          ) %>%
          select(datetime, sec, presAtm, relative_humidity) 
        # here at some point I can filter to make sure we aren't including bad pressure and RH values 
    } else {
      
      warning(sprintf("No WXT file found in folder %s", reading_folder))
      df_reading <- tibble(
        datetime = df_5hz$datetime,
        sec = floor_date(df_5hz$datetime, "1 sec"),
        presAtm = rnorm(nrow(df_5hz), 101325, 1000),
        relative_humidity = rnorm(nrow(df_5hz), 50, 20)
      )
    }
  
  # ----------------------------------------------------------
  # JOIN ALL DATASETS
  # ----------------------------------------------------------
  
  df_5hz_final <- df_5hz %>%
    rename_all(tolower) %>% 
    mutate(sec = floor_date(datetime, "1 sec")) 
  
  # Convert to data.tables
  dt_5hz <- as.data.table(df_5hz_final)
  dt_cal <- as.data.table(df_cal %>% select(sec, ch1_zero, ch2_zero, ch1_sens, ch2_sens, ce))
  dt_reading <- as.data.table(df_reading %>% select(sec, presAtm, relative_humidity))
  
  # --- Join calibration data by exact sec ---
  setkey(dt_5hz, sec)
  setkey(dt_cal, sec)
  dt_5hz <- dt_cal[dt_5hz]  # exact join on sec
  
  # --- Join reading data by exact sec ---
  setkey(dt_5hz, sec)
  setkey(dt_reading, sec)
  dt_5hz <- dt_reading[dt_5hz]
  
  # Convert back to tibble for dplyr manipulations
  df_5hz_final <- as_tibble(dt_5hz) %>%
    mutate(
      ch1_hz = ifelse(ch1_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch1_hz),
      ch2_hz = ifelse(ch2_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch2_hz),
      ch1_hz  = (ch1_hz - ch1_zero) / ch1_sens * 1e-3,
      ch2_hz = (((ch2_hz - ch2_zero) / ch2_sens * 1e-3) - ch1_hz) / ce) %>% 
    mutate(unixTime = as.numeric(datetime), 
           veloXaxs = -vv, 
           veloYaxs = u, 
           veloZaxs = w, 
           tempAir = temp_sonic, 
           presAtm = presAtm,
           relative_humidity = relative_humidity,
           distZaxsAbl = 1500, 
           distZaxsMeas = 190, 
           rtioMoleDryH2o = eddy4R.york::def.rtio.mole.h2o.temp.pres.rh(tempAir, presAtm, relative_humidity)
    ) %>%
    select(
      unixTime, veloXaxs, veloYaxs, veloZaxs, tempAir, presAtm,
      distZaxsAbl, distZaxsMeas, rtioMoleDryH2o,
      rtioMoleDryNO = ch1_hz, rtioMoleDryNO2 = ch2_hz, relative_humidity
    )
  
  # --- save in same structure as input ---
  out_file <- file.path(out_root, 
                        year_val, month_val, basename(file_5hz))
  
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
  
  write_csv(df_5hz_final, out_file)
  #}
  
}else{
  print("no Reading Data")
}
  
  
  
  
  
  
  
  