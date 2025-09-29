# getting 1 Hz calibration data

# library
library(tidyverse)
library(arrow)
library(future)
library(purrr)
library(furrr)

# read in the 36 hour calibrations

cal_data <- read.csv("data/data/coeffs_data.csv") |> 
  mutate(date = as_datetime(date, tz = "UTC")) %>% 
  mutate(period = case_when(
    date < as.Date("2023-10-19") ~ "period1",
    date >= as.Date("2023-10-19") & date < as.Date("2024-02-08") ~ "period2",
    date >= as.Date("2024-02-08") & date < as.Date("2024-10-28") ~ "period3",
    date >= as.Date("2024-10-28") & date < as.Date("2025-02-03") ~ "period4",
    date >= as.Date("2025-02-03") & date < as.Date("2025-03-18") ~ "period5",
    date >= as.Date("2025-03-18") & date < as.Date("2025-06-05") ~ "period6",
    date >= as.Date("2025-06-05") ~ "period7",
    TRUE ~ NA_character_  # any calibration outside periods cannot be corrected
  )) %>% 
  filter(!is.na(date)) %>% 
    mutate(year_month = floor_date(date, "month")) %>% 
  rename("reaction_cell_pressure" = av_rxn_vessel_pressure)

# get linear models for sensitivity and pressure 

  lm_list_ch1 <- list(
    period1 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period1")),
    period2 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period2")),
    period3 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period3")),
    period4 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period4")),
    period5 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period5")),
    period6 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period6")),
    period7 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period7"))
  )
  
  
  lm_list_ch2 <- list(
    period1 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period1")),
    period2 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period2")),
    period3 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period3")),
    period4 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period4")),
    period5 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period5")),
    period6 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period6")),
    period7 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(cal_data, period == "period7"))
  )

  get_period <- function(datetime) {
    case_when(
      datetime < as.Date("2023-10-19") ~ "period1",
      datetime >= as.Date("2023-10-19") & datetime < as.Date("2024-02-08") ~ "period2",
      datetime >= as.Date("2024-02-08") & datetime < as.Date("2024-10-28") ~ "period3",
      datetime >= as.Date("2024-10-28") & datetime < as.Date("2025-02-03") ~ "period4",
      datetime >= as.Date("2025-02-03") & datetime < as.Date("2025-03-18") ~ "period5",
      datetime >= as.Date("2025-03-18") & datetime < as.Date("2025-06-05") ~ "period6",
      datetime >= as.Date("2025-06-05") ~ "period7",
      TRUE ~ NA_character_
    )
  }
  
  # Extract coefficients for vectorized prediction
  coef_tbl_ch1 <- map_dfr(names(lm_list_ch1), function(p) {
    b <- coef(lm_list_ch1[[p]])
    tibble(period = p, intercept_ch1 = b[1], slope_ch1 = b[2])
  })
  
  coef_tbl_ch2 <- map_dfr(names(lm_list_ch2), function(p) {
    b <- coef(lm_list_ch2[[p]])
    tibble(period = p, intercept_ch2 = b[1], slope_ch2 = b[2])
  })
  
  
  ce_interp <- cal_data %>%
    select(date, ce) %>%
    arrange(date)
  
  # processing the monthly param data to get 1 Hz calibration data 
  
  process_month <- function(f, out_dir = "data/data/1_hz_cal_data_parquets/") {
    message("Processing: ", f)
    
    # Read parquet for one month
    df <- open_dataset(f, format = "parquet") %>%
      collect() %>%
      mutate(datetime = as_datetime(unix_time, tz = "UTC")) %>%
      arrange(datetime)
    
    if (nrow(df) == 0) return(NULL)
    
    # --- interpolate CE for this month only
    df <- df %>%
      mutate(
        ce = approx(ce_interp$date, ce_interp$ce, xout = datetime, rule = 2)$y
      )
    
    # --- add period
    df <- df %>%
      mutate(period = get_period(datetime))
    
    # --- join model coefficients
    df <- df %>%
      left_join(coef_tbl_ch1, by = "period") %>%
      left_join(coef_tbl_ch2, by = "period") %>%
      mutate(
        ch1_sens = intercept_ch1 + slope_ch1 * reaction_cell_pressure,
        ch2_sens = intercept_ch2 + slope_ch2 * reaction_cell_pressure
      ) %>%
      select(-intercept_ch1, -slope_ch1, -intercept_ch2, -slope_ch2)
    
    zero_data <- df %>%
      filter(zero_valve_1 == 1.0, zero_valve_2 == 1.0) %>%
      mutate(second = second(datetime)) %>%
      filter(between(second, 5, 15)) %>%
      select(date = datetime, CH1_Hz, CH2_Hz) %>%
      arrange(date)
    
    if (nrow(zero_data) > 0) {
      df <- df %>%
        mutate(
          ch1_zero = approx(zero_data$date, zero_data$CH1_Hz, xout = datetime, rule = 2)$y,
          ch2_zero = approx(zero_data$date, zero_data$CH2_Hz, xout = datetime, rule = 2)$y
        )
    } else {
      df <- df %>%
        mutate(ch1_zero = NA_real_, ch2_zero = NA_real_)
    }
    
    # --- figure out year for subfolder
    year_val <- year(min(df$datetime, na.rm = TRUE))
    year_dir <- file.path(out_dir, as.character(year_val))
    dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
    
    # --- write calibrated parquet into year subfolder
    out_file <- file.path(year_dir, basename(f))
    write_parquet(df, out_file)
    
    return(out_file)
  }
  
  # -------------------------
  # 4. Run over all files (parallel)
  # -------------------------
  data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params"
  param_files <- list.files(file.path(data_root),
                            full.names = T, 
                            pattern = "\\.parquet$",
                            recursive = T)
  
  plan(multisession, workers = 4)  # adjust cores
  future_walk(param_files, process_month)

  
  
  
  # checking
  file_path <- "data/data/1_hz_cal_data_parquets/2023/NOx_params_2023_02-0.parquet"
  
  # Read the file
  param_data <- arrow::read_parquet(file_path)
  
  # Quick overview of the dataset
  glimpse(param_data) 
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  

  