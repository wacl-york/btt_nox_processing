

#!/usr/bin/env Rscript
# creating concentration files - TEST VERSION (Sona vs ERA5)

library(tidyverse)
library(arrow)
library(data.table)
library(lubridate)

# --- waclr dependency ---
parse_unix_time <- function(x, tz = "UTC", origin = "1970-01-01") {
  if (tz == "nz") tz <- "Etc/GMT-12"
  as.POSIXct(x, tz = tz, origin = origin)
}

parse_excel_date <- function (x, tz = "UTC", type = "windows") {
  type <- stringr::str_to_lower(type); type <- stringr::str_replace_all(type, "\\.| ", "_")
  if (!type %in% c("windows", "os_x_2007")) stop("Type must be 'windows' or 'os_x_2007'")
  if (!class(x) == "numeric") x <- as.numeric(x)
  if (type == "windows") x <- (x - 25569) * 86400
  if (type == "os_x_2007") x <- (x - 24107) * 86400
  parse_unix_time(x, tz = tz)
}

# --- Mock eddy4R function for local testing ---
# Use this if you are not in the container.
def_rtio_mole_h2o_local <- function(T_kelvin, P_pa, RH_pct) {
  T_c <- T_kelvin - 273.15
  es <- 611.2 * exp((17.67 * T_c) / (T_c + 243.5))
  e <- (RH_pct / 100) * es
  return(e / (P_pa - e))
}

# --- data roots ---
data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/five_hz/2020/10"
sona_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/sona_data/2020" # Adjust year as needed
era5_path <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/modelled_data/pressure_corrected.csv"
cal_root  <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data"
out_root_sona <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_test/in/test_week_sona"
out_root_era5 <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_test/in/test_week_era5"

print("getting file list")
all_files = system(paste("find", data_root," -type f -name '*.csv'"), intern = TRUE) %>% sort()

# SET i MANUALLY FOR LINE-BY-LINE TEST
# i = 20
args = commandArgs(trailingOnly = TRUE)[1]
if(is.na(args)) { i = 20 } else { i = as.numeric(args)+1 }

i = 300
file_5hz <- all_files[i]

# --- 1. READ MASTER 5Hz ---
df_5hz <- read_csv(file_5hz, show_col_types = FALSE) %>% 
  mutate(datetime = parse_excel_date(TheTime, tz = "UTC")) %>% 
  ungroup() %>% 
  arrange(datetime)

if (nrow(df_5hz) == 0) stop("File is empty")

year_val  <- year(min(df_5hz$datetime))
month_val <- sprintf("%02d", month(min(df_5hz$datetime)))
day_val   <- sprintf("%02d", day(min(df_5hz$datetime)))
hour_val  <- sprintf("%02d", hour(min(df_5hz$datetime))) 
min_val   <- sprintf("%02d", minute(min(df_5hz$datetime)))

# --- 2. READ CALIBRATION ---
cal_file <- file.path(cal_root, year_val, paste0("param_", year_val, "_", month_val, ".parquet"))
df_cal <- open_dataset(cal_file, format = "parquet") %>%
  collect() %>%
  mutate(sec = floor_date(datetime, "1 sec")) %>%
  distinct(sec, .keep_all = TRUE) 

# --- 3. READ ERA5 (Master Pressure/RH Source) ---
df_era5 <- read_csv(era5_path, show_col_types = FALSE) %>%
  mutate(datetime = as.POSIXct(valid_time, tz = "UTC"))

# --- 4. READ & AVERAGE SONIC 20Hz ---
# Construct sona filename from the 5Hz filename
sona_filename <- sprintf("Sonic_Licor_BT_%04d%s%s_%s%s.sona", 
                         year_val, month_val, day_val, hour_val, min_val)

file_sona <- file.path(sona_root, sona_filename)

if(!file.exists(file_sona)) {
  # If the file doesn't exist, try looking for just the top of the hour (HH00) 
  # in case the 5Hz starts at a weird minute but the Sona file is hourly.
  sona_filename_hourly <- sprintf("Sonic_Licor_BT_%04d%s%s_%s00.sona", 
                                  year_val, month_val, day_val, hour_val)
  file_sona <- file.path(sona_root, sona_filename_hourly)
}

if(!file.exists(file_sona)) {
  stop(paste("Sona file not found. Checked:", sona_filename, "and hourly variant."))
}

message(sprintf("Loading Sona file: %s", basename(file_sona)))

start_time_sona <- ymd_hm(sprintf("%04d%s%s %s%s", 
                                  year_val, month_val, day_val, hour_val, min_val), 
                          tz = "UTC")

df_sona_avg <- vroom::vroom(file_sona, show_col_types = FALSE) %>%
  mutate(datetime = start_time_sona + ((row_number() - 1) * 0.05)) %>%
  mutate(dt_5hz = floor_date(datetime, "0.2 sec")) 
  group_by(dt_5hz) %>%
  summarise(across(c(h2o_ppthou, co2_ppm), ~mean(.x, na.rm = TRUE))) %>%
  rename(datetime = dt_5hz)

# need to add here that if h2oppthou is > etc then need to be NA and use modelled data or interpolate 

# --- 5. JOINING (The original data.table way) ---
df_5hz_final <- df_5hz %>% rename_all(tolower) %>% mutate(sec = floor_date(datetime, "1 sec")) 

dt_5hz  <- as.data.table(df_5hz_final)
dt_cal  <- as.data.table(df_cal %>% select(sec, ch1_zero, ch2_zero, ch1_sens, ch2_sens, ce))
dt_era5 <- as.data.table(df_era5)
dt_sona <- as.data.table(df_sona_avg)

setkey(dt_5hz, sec); setkey(dt_cal, sec)
dt_5hz <- dt_cal[dt_5hz] # Exact join on seconds for calibration

setkey(dt_5hz, datetime); setkey(dt_era5, datetime); setkey(dt_sona, datetime)
dt_5hz <- dt_era5[dt_5hz, roll = "nearest"]
dt_5hz <- dt_sona[dt_5hz, roll = "nearest"]

# Cleanup datetime names after join
if ("i.datetime" %in% names(dt_5hz)) {
  dt_5hz[, datetime := i.datetime]; dt_5hz[, i.datetime := NULL]
}

# --- 6. FINAL CALCULATIONS ---
df_final_all <- as_tibble(dt_5hz) %>%
  mutate(
    # Corrected Met Logic
    tempAir = (temp_sonic^2)/403,
    presAtm = pres_190m_pa, 
    # Calculate height-corrected RH from ERA5
    e_pa  = 611.2 * exp(17.67 * (d2m - 273.15) / (d2m - 273.15 + 243.5)),
    es_pa = 611.2 * exp(17.67 * (tempAir - 273.15) / (tempAir - 273.15 + 243.5)),
    relative_humidity = (e_pa / es_pa) * 100,
    
    # NOy Concentration logic
    ch1_hz = ifelse(ch1_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch1_hz),
    ch2_hz = ifelse(ch2_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch2_hz),
    rtioMoleDryNO  = ((ch1_hz - ch1_zero) / ch1_sens) * 1e-12,
    rtioMoleDryNO2 = ((ch2_hz - ch2_zero) / ch2_sens) * 1e-12,
    rtioMoleDryCO2 = co2_ppm * 1e-6,
    
    # Water Path A: Sona (Dry corrected)
    rtio_wet_sona = h2o_ppthou / 100,
    rtioMoleDryH2o_sona = rtio_wet_sona / (1 - rtio_wet_sona),
    
    # Water Path B: ERA5
    # rtioMoleDryH2o_era5 = eddy4R.york::def.rtio.mole.h2o.temp.pres.rh(tempAir, presAtm, relative_humidity)
    rtioMoleDryH2o_era5 = def_rtio_mole_h2o_local(tempAir, presAtm, relative_humidity)
  ) %>%
  mutate(
    unixTime = as.numeric(datetime),
    veloXaxs = -vv, veloYaxs = u, veloZaxs = w,
    distZaxsAbl = 1500, distZaxsMeas = 177
  )

# --- 7. SELECT AND SAVE ---
final_cols <- c("unixTime", "veloXaxs", "veloYaxs", "veloZaxs", "tempAir", "presAtm",
                "distZaxsAbl", "distZaxsMeas", "rtioMoleDryH2o", "rtioMoleDryCO2",
                "rtioMoleDryNO", "rtioMoleDryNO2", "ce")

# Sona Version
df_out_sona <- df_final_all %>% mutate(rtioMoleDryH2o = rtioMoleDryH2o_sona) %>% select(all_of(final_cols))
# ERA5 Version
df_out_era5 <- df_final_all %>% mutate(rtioMoleDryH2o = rtioMoleDryH2o_era5) %>% select(all_of(final_cols))

write_csv(df_out_sona, file.path(out_root_sona, basename(file_5hz)))
write_csv(df_out_era5, file.path(out_root_era5, basename(file_5hz)))

message("Done with test file.")