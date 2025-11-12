#  creating concentration files 

# library
library(tidyverse)
library(arrow)
library(data.table)


# waclr dependency ####

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

data_root <- "btt_processing/raw_data/five_hz"
out_root <- "btt_processing/processing/5Hz_input_files"
met_root <- "btt_processing/raw_data/reading_met_data"
cal_root  <- "btt_processing/processing/1Hz_cal_data"


all_files <- list.files(data_root, full.names = TRUE, pattern = "\\.csv$", recursive = TRUE)

#test_files <- all_files[24000:24001]

# processing function ####
args = commandArgs(trailingOnly = TRUE)[1]
i = as.numeric(args)+1


#for (i in seq_along(all_files)) {

  i <- 1 #placeholder if not using batch
  
  file_5hz <- all_files[i]
  
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
  # READ MATCHING READING DATA FILE(S)
  # ----------------------------------------------------------
  folder_name <- paste0(day_val, month_val, year_val)
  reading_folder <- file.path(met_root, year_val, folder_name)
  
  pattern <- sprintf("HiRes%d[0-9]{3}_%s\\.txt$", year_val, hour_val)
  reading_candidates <- list.files(reading_folder, pattern = pattern, full.names = TRUE)
  
  if (length(reading_candidates) > 0) {
    reading_file <- reading_candidates[1]
    message(sprintf("Reading file found: %s", basename(reading_file)))
    
    # Read with no headers, allowing missing values
    df_reading <- read_csv(
      reading_file,
      col_names = FALSE,
      show_col_types = FALSE,
      na = c("", "NA")
    )
    
    # Make sure we have enough columns
    if (ncol(df_reading) >= 9) {
      df_reading <- df_reading %>%
        rename(col1 = X1, col2 = X2, col3 = X3) %>% 
        mutate(
          # Extract hour/minute from col2 (e.g. 0600 → 06, 00)
          hh = as.numeric(str_sub(sprintf("%04d", as.integer(col2)), 1, 2)),
          mm = as.numeric(str_sub(sprintf("%04d", as.integer(col2)), 3, 4)),
          ss = as.numeric(col3),
          
          # Build proper POSIXct timestamp with sub-second precision
          datetime = as.POSIXct(
            paste0(
              day_val, "-", month_val, "-", year_val, " ",
              sprintf("%02d:%02d:%06.3f", hh, mm, ss)
            ),
            format = "%d-%m-%Y %H:%M:%OS", tz = "UTC"
          ),
          
          sec = floor_date(datetime, "1 sec"),
          
          # Temporary assumptions for now
          relative_humidity = as.numeric(X8),
          presAtm = as.numeric(X9)
        ) %>%
        select(datetime, sec, presAtm, relative_humidity)
      
    } else {
      warning(sprintf("File %s has too few columns (<9)", reading_file))
      df_reading <- tibble(sec = as.POSIXct(character()), presAtm = NA, relative_humidity = NA)
    }
    
  } else {
    warning(sprintf("No matching reading file found for hour %s in %s", hour_val, reading_folder))
    df_reading <- tibble(sec = as.POSIXct(character()), presAtm = NA, relative_humidity = NA)
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
  dt_reading <- as.data.table(df_reading %>% select(datetime, presAtm, relative_humidity))
  
  # --- Join calibration data by exact sec ---
  setkey(dt_5hz, sec)
  setkey(dt_cal, sec)
  dt_5hz <- dt_cal[dt_5hz]  # exact join on sec
  
  # --- Join reading data by nearest timestamp ---
  setkey(dt_5hz, datetime)
  setkey(dt_reading, datetime)
  dt_5hz <- dt_reading[dt_5hz, roll = "nearest"]  # nearest-neighbor join
  
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
             rtioMoleDryH2o = eddy4R::rtio.mole.h2o.temp.pres.rh(tempAir, presAtm, relative_humidity)
    ) %>%
    select(
      unixTime, veloXaxs, veloYaxs, veloZaxs, tempAir, presAtm,
      distZaxsAbl, distZaxsMeas, rtioMoleDryH2o,
      ch1_hz, ch2_hz, relative_humidity
    )
  
  # --- save in same structure as input ---
  out_file <- file.path(out_root, 
                        year_val, month_val, basename(file_5hz))
  
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
  
  write_csv(df_5hz_final, out_file)
#}









