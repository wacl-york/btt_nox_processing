#  creating concentration files 

# library
library(tidyverse)
library(arrow)
library(data.table)
library(zoo)

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




args = commandArgs(trailingOnly = TRUE)      # keep all args
i = as.numeric(args[1]) + 1
year_arg = as.numeric(args[2])

# data roots ####

# might have to make changes here and also to the sbatch files as we have moved raw_data to longship

data_root <- paste0("/data/raw_data/five_hz/", year_arg)
out_root <- "/data/processing/ec_2/in"
cal_root  <- "/data/processing/1Hz_cal_data"
ERA5_root <- "/data/processing/modelled_data/pressure_corrected.csv"
sona_root <- "/data/raw_data/sona_data"

print("getting file list")

all_files = system(paste("find", data_root," -type f -name '*.csv'"), intern = TRUE) %>% 
  sort()

file_5hz <- all_files[i]

# --- read the hourly 5Hz CSV ---
df_5hz <- read_csv(file_5hz) %>% 
  mutate(datetime = parse_excel_date(TheTime, tz = "UTC")) %>% 
  ungroup() %>% 
  arrange(datetime) %>% 
  select(-c(CH1_sens, CH2_sens))

df_5hz_final <- df_5hz %>%
  rename_all(tolower) %>% 
  mutate(sec = floor_date(datetime, "1 sec")) 

if (nrow(df_5hz_final) == 0) quit(save = "no")

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
# READ ERA5 MODELLED DATA
# ----------------------------------------------------------

start_time <- min(df_5hz$datetime)
end_time   <- max(df_5hz$datetime)

era5_start <- floor_date(start_time, "sec") - 3600  # minus 1 hour
era5_end   <- floor_date(end_time, "sec")   + 3600  # plus 1 hour

ERA5_data <- read_csv(ERA5_root) %>%
  mutate(datetime = as.POSIXct(valid_time, tz = "UTC")) %>%
  filter(datetime >= era5_start & datetime <= era5_end) %>%
  mutate(sec = floor_date(datetime, "sec")) %>% 
  select(sec, rtioMoleDryH2O, pres_190m_pa)

ERA5_all_times <- data.table(sec = sort(unique(c(
  df_5hz_final$sec,
  ERA5_data$sec
))))

ERA5_all_times <- merge(ERA5_all_times, ERA5_data, by = "sec", all.x = TRUE)

ERA5_all_times[, rtioMoleDryH2O := na.approx(rtioMoleDryH2O, x = sec, na.rm = FALSE)]
ERA5_all_times[, pres_190m_pa   := na.approx(pres_190m_pa,   x = sec, na.rm = FALSE)]

# ----------------------------------------------------------
# READ SONA 20Hz DATA FOR CO2
# ----------------------------------------------------------

sona_file <- file.path(
  sona_root,
  year_val,
  sprintf("Sonic_Licor_BT_%s%s%s_%s00.sona", year_val, month_val, day_val, hour_val)
)

# Always produce a dt_sona — NA-filled if file missing or column absent
read_sona_co2 <- function(sona_file, file_start) {
  
  df <- read_csv(sona_file, show_col_types = FALSE) %>%
    rename_with(~ "co2", any_of(c("co2_ppm", "co2_dry_ppm")))
  
  if (!"co2" %in% names(df)) {
    warning("No CO2 column found in ", sona_file)
    return(NULL)
  }
  
  df %>%
    mutate(
      # 0.2s windows: every 4 rows at 20Hz
      window_02s = file_start + floor((row_number() - 1) / 4) * 0.2,
    ) %>%
    group_by(window_02s) %>%
    summarise(co2_ppm = mean(co2, na.rm = TRUE), .groups = "drop") %>%
    # quality filter
    mutate(co2_ppm = ifelse(co2_ppm > 400 & co2_ppm < 600, 
                            co2_ppm, NA_real_)) %>%
    rename(datetime = window_02s)
}

file_start <- ymd_hms(
  paste0(year_val, "-", month_val, "-", day_val, " ", hour_val, ":00:00"),
  tz = "UTC"
)

dt_sona <- if (file.exists(sona_file)) {
  result <- tryCatch(
    read_sona_co2(sona_file, file_start),
    error = function(e) { warning("Failed to read SONA file: ", e$message); NULL }
  )
  if (!is.null(result)) as.data.table(result) else NULL
} else {
  warning("SONA file not found: ", sona_file)
  NULL
}


# ----------------------------------------------------------
# JOIN ALL DATASETS
# ----------------------------------------------------------

# Convert to data.tables
dt_5hz <- as.data.table(df_5hz_final)
dt_cal <- as.data.table(df_cal %>% select(sec, ch1_zero, ch2_zero, ch1_sens, ch2_sens, ce))
dt_ERA5 <- as.data.table(ERA5_all_times)

# --- Join calibration data by exact sec ---
setkey(dt_5hz, sec)
setkey(dt_cal, sec)
dt_5hz <- dt_cal[dt_5hz]  # exact join on sec

setkey(dt_ERA5, sec)
setkey(dt_5hz, sec)

dt_5hz <- dt_ERA5[dt_5hz]

if (!is.null(dt_sona)) {
  setkey(dt_sona, datetime)
  setkey(dt_5hz, datetime)
  dt_5hz <- dt_sona[dt_5hz, roll = "nearest"]
} else {
  dt_5hz[, co2_ppm := NA_real_]
}


# Convert back to tibble for dplyr manipulations
df_5hz_final <- as_tibble(dt_5hz) %>%
  mutate(
    ch1_hz = ifelse(ch1_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch1_hz),
    ch2_hz = ifelse(ch2_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch2_hz),
    ch1_hz  = ((ch1_hz - ch1_zero) / ch1_sens) * 1e-12,
    ch2_hz = ((ch2_hz - ch2_zero) / ch2_sens)* 1e-12) %>% 
  mutate(unixTime = as.numeric(datetime), 
         veloXaxs = -vv, 
         veloYaxs = u, 
         veloZaxs = w, 
         tempAir = (temp_sonic^2)/403, 
         presAtm = pres_190m_pa,
         rtioMoleDryH2o = rtioMoleDryH2O,
         rtioMoleDryco2 = co2_ppm/1e6,
         distZaxsAbl = 1500, 
         distZaxsMeas = 177) %>% 
  select(
    unixTime, veloXaxs, veloYaxs, veloZaxs, tempAir, presAtm,
    distZaxsAbl, distZaxsMeas, rtioMoleDryH2o,
    rtioMoleDryNO = ch1_hz, rtioMoleDryNO2 = ch2_hz, ce,rtioMoleDryco2
  )

# --- save in same structure as input ---
out_file <- file.path(out_root, 
                      year_val, month_val, basename(file_5hz))

dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)

write_csv(df_5hz_final, out_file)
