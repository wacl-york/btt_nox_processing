#  creating concentration files new version

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

# data_root <- "/data/raw_data/five_hz/2025"
# out_root <- "/data/processing/ec/in_version2"
# met_root <- "/data/raw_data/sona_data"
# cal_root  <- "/data/processing/1Hz_cal_data"
# sam_root <- "/data/sam_input_data"

data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/five_hz/2025"
out_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec/in_version2"
met_root2 <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/sona_data"
cal_root  <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data"
sam_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/sam_input_data"

print("getting file list")

all_files = system(paste("find", data_root," -type f -name '*.csv'"), intern = TRUE) %>% 
  sort()

args = commandArgs(trailingOnly = TRUE)[1]
i = as.numeric(args)+1

i = 4000

file_5hz <- all_files[i]

# --- read the hourly 5Hz CSV ---
df_5hz <- read_csv(file_5hz) %>% 
  mutate(datetime = parse_excel_date(TheTime, tz = "UTC")) %>% 
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
# READ SONA FILE
# ----------------------------------------------------------

sona_file <- file.path(
  met_root2,
  year_val,
  sprintf("Sonic_Licor_BT_%s%s%s_%s00.sona",
          year_val, month_val, day_val, hour_val)
)

df_sona <- read_csv(sona_file, show_col_types = FALSE)

start_time <- ymd_hm(
  stringr::str_extract(sona_file, "\\d{8}_\\d{4}"),
  tz = "UTC"
)

start_unix <- as.numeric(start_time)

freq <- 20
dt <- 1 / freq  # 0.05 sec

df_sona <- df_sona %>%
  mutate(
    unixTime = start_unix + (row_number() - 1) * dt
  )


df_sona_5hz <- df_sona %>%
  mutate(time_5hz = floor(unixTime / 0.2) * 0.2) %>%
  group_by(time_5hz) %>%
  summarise(
    #u = mean(u, na.rm = TRUE),
    #v    = mean(v, na.rm = TRUE),
    #w    = mean(w, na.rm = TRUE),
    co2_ppm    = mean(co2_ppm, na.rm = TRUE),
    h2o_ppthou    = mean(h2o_ppthou, na.rm = TRUE),
    #SoS    = mean(SoS, na.rm = TRUE)
    
  ) %>% 
  mutate(datetime = as.POSIXct(time_5hz, tz = "UTC"))
  




