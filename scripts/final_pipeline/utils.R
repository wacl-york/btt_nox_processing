logs_path = function(...){
  
  # here::here(readLines(here::here("data_path.txt"), n = 1), "logs", ...)
  here::here("logs", ...)
}

data_path = function(...){
  
  # here::here(readLines(here::here("data_path.txt"), n = 1), "data", ...)
  here::here("data", ...)
}

connect_to_db = function(read_only = TRUE){
  
  con = DBI::dbConnect(
    drv = duckdb::duckdb(),
    dbdir = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_2/duckdb/ec_BT.duckdb",
    read_only = read_only
  )
  
  con
  
}


library(dplyr)

get_period <- function(datetime) {
  case_when(
    datetime <= as.POSIXct("2020-10-13 01:00:00", tz = "UTC") ~ "period1",
    datetime >= as.POSIXct("2020-10-13 02:00:00", tz = "UTC") & datetime < as.POSIXct("2021-01-11", tz = "UTC") ~ "period2",
    datetime >= as.POSIXct("2021-01-11", tz = "UTC") & datetime < as.POSIXct("2021-06-11", tz = "UTC") ~ "period3",
    datetime >= as.POSIXct("2021-06-11", tz = "UTC") & datetime < as.POSIXct("2021-12-31", tz = "UTC") ~ "period4",
    datetime >= as.POSIXct("2021-12-31", tz = "UTC") & datetime < as.POSIXct("2022-11-01", tz = "UTC") ~ "period5",
    datetime >= as.POSIXct("2022-11-01", tz = "UTC") & datetime < as.POSIXct("2023-10-19", tz = "UTC") ~ "period6",
    datetime >= as.POSIXct("2023-10-19", tz = "UTC") & datetime < as.POSIXct("2024-02-08", tz = "UTC") ~ "period7",
    datetime >= as.POSIXct("2024-02-08", tz = "UTC") & datetime < as.POSIXct("2025-02-03", tz = "UTC") ~ "period8",
    datetime >= as.POSIXct("2025-02-03", tz = "UTC") & datetime < as.POSIXct("2025-03-18", tz = "UTC") ~ "period9",
    datetime >= as.POSIXct("2025-03-18", tz = "UTC") & datetime < as.POSIXct("2025-06-05", tz = "UTC") ~ "period10",
    datetime >= as.POSIXct("2025-06-05", tz = "UTC") & datetime < as.POSIXct("2025-11-13", tz = "UTC") ~ "period11",
    datetime >= as.POSIXct("2025-11-13", tz = "UTC") & datetime < as.POSIXct("2025-12-30", tz = "UTC") ~ "period12",
    datetime >= as.POSIXct("2025-12-30", tz = "UTC") ~ "period13",
    TRUE ~ NA_character_
  )
}
