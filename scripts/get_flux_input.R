
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



# functions ####

backcalc_RH <- function(T_air, pressure, FD_mole_H2O) {
  
  # Molecular weight ratio
  mvmd <- 18.02 / 28.97
  
  # Convert mole ratio to specific humidity q
  q <- FD_mole_H2O * mvmd
  
  # Solve for vapor pressure e [hPa]
  e <- q * pressure / (0.62198 + 0.37802 * q)
  
  # Magnus formula for Esat
  cM1 <- 6.11
  cM2 <- 17.08
  cM3 <- 234.18
  
  T_C <- T_air - 273.15
  
  E <- cM1 * exp((cM2 * T_C) / (cM3 + T_C))
  
  # RH as fraction (0–1)
  RH <- e / E
  
  return(RH)
}

# data roots ####

data_root <- "/data/raw_data/five_hz"
out_root <- "/data/processing/5Hz_input_files"
met_root <- "/data/processing/met_data_formatted"
cal_root  <- "/data/processing/1Hz_cal_data"
sam_root <- "data/raw_data/sam_input_data"

print("getting file list")

all_files = system(paste("find", data_root, " -type f -name '*.csv'"), intern = TRUE) %>% 
  sort()

args = commandArgs(trailingOnly = TRUE)[1]
i = as.numeric(args)+1

file_5hz <- all_files[i]

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

if (file.exists(reading_file)) {
  
  message(sprintf("Reading file found: %s", basename(reading_file)))
  source_type <- "WXT"
  
  df_reading <- read_csv(reading_file)
  
  med_pres <- median(df_reading$X11, na.rm = TRUE)
  
  # -------------------------------------------------------
  # CASE A: WXT pressure invalid
  # -------------------------------------------------------
  if (is.na(med_pres) || med_pres < 100) {
    
    warning("WXT pressure seems invalid (<100). Using placeholder values.")
    
    df_met <- tibble(
      datetime = df_5hz$datetime,
      sec      = floor_date(df_5hz$datetime, "1 sec"),
      presAtm  = rnorm(nrow(df_5hz), 101325, 1000),
      relative_humidity = rnorm(nrow(df_5hz), 50, 20)
    )
    
    join_type <- "datetime"
    
  } else {
    
    # -------------------------------------------------------
    # CASE B: WXT pressure OK → detailed QC
    # -------------------------------------------------------
    df_met <- df_reading %>%
      mutate(
        sec = floor_date(datetime, "1 sec"),
        presAtm = X11,
        relative_humidity = X10
      ) %>%
      select(datetime, sec, presAtm, relative_humidity) %>%
      mutate(
        presAtm = ifelse(presAtm < 700 | presAtm > 100000, NA, presAtm)
      ) %>% 
      mutate(presAtm = (zoo::na.locf(presAtm, na.rm = FALSE))*100) # the reading pressure data has 100X lower than Sam's input files
    
    join_type <- "sec"
  }
  
} else {
  
  # -------------------------------------------------------
  # FALLBACK: SAM INPUT FILE
  # -------------------------------------------------------
  warning(sprintf("No WXT file found in %s — using Sam file fallback", reading_folder))
  
  doy <- yday(min(df_5hz$datetime))
  doy_folder <- sprintf("%03d", doy)
  
  sam_folder <- file.path(sam_root, year_val, doy_folder)
  
  pattern_sam <- sprintf("NOx_5Hz_%02d_%02d_%02d_%02d0000.csv",
                         yy %% 100, mm, dd, as.numeric(hour_val))
  
  sam_file <- file.path(sam_folder, pattern_sam)
  
  if (!file.exists(sam_file)) {
    
    warning("Fallback SAM file does not exist — using NA placeholders.")
    
    source_type <- "PLACEHOLDER"
    
    df_met <- tibble(
      datetime = df_5hz$datetime,
      sec = floor_date(df_5hz$datetime, "1 sec"),
      presAtm = rnorm(nrow(df_5hz), 101325, 1000),
      relative_humidity = rnorm(nrow(df_5hz), 50, 20)
    )
    
    join_type <- "datetime"
    
  } else {
    
    message(sprintf("Using fallback Sam file: %s", basename(sam_file)))
    source_type <- "SAM"
    
    df_sam <- read_csv(sam_file, show_col_types = FALSE) %>%
      mutate(
        datetime = as.POSIXct(date, format = "%Y-%m-%d %H:%M:OS", tz = "UTC"),
        relative_humidity = backcalc_RH(T_air, p_air, FD_mole_H2O),
        presAtm = p_air
      )
    
    df_met <- df_sam %>%
      select(datetime, presAtm, relative_humidity) %>%
      distinct(datetime, .keep_all = TRUE)
    
    join_type <- "datetime"
  }
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
dt_met <- as.data.table(df_met)

# --- Join calibration data by exact sec ---
setkey(dt_5hz, sec)
setkey(dt_cal, sec)
dt_5hz <- dt_cal[dt_5hz]  # exact join on sec

if (join_type == "sec") {
  dt_met <- dt_met[, .(sec, presAtm, relative_humidity)]
  setkey(dt_met, sec)
  
  dt_5hz[, sec := floor_date(datetime, "1 sec")]
  setkey(dt_5hz, sec)
  
  dt_5hz <- dt_met[dt_5hz]
}

if (join_type == "datetime") {
  setkey(dt_met, datetime)
  dt_5hz <- dt_met[dt_5hz, roll = "nearest"]
}

# Convert back to tibble for dplyr manipulations
df_5hz_final <- as_tibble(dt_5hz) %>%
  mutate(
    ch1_hz = ifelse(ch1_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch1_hz),
    ch2_hz = ifelse(ch2_hz < 0 | no_valve == 1 | zero_valve_1 == 1 | no_cal == 1, NA, ch2_hz),
    ch1_hz  = (ch1_hz - ch1_zero) / ch1_sens * 1e-3,
    ch2_hz = (((ch2_hz - ch2_zero) / ch2_sens * 1e-3) - ch1_hz)) %>% 
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
    rtioMoleDryNO = ch1_hz, rtioMoleDryNO2 = ch2_hz, relative_humidity, ce
  )

# --- save in same structure as input ---
out_file <- file.path(out_root, 
                      year_val, month_val, basename(file_5hz))

dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)

write_csv(df_5hz_final, out_file)
