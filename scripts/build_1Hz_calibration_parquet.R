# getting 1 Hz calibration data

# library
library(tidyverse)
library(arrow)
library(future)
library(purrr)
library(furrr)
library(waclr)

# read in the 36 hour calibrations

files_coefficients <- list.files(path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/calibration_data/", 
                                 full.names = T, 
                                 pattern = "coefficients",
                                 recursive = T)


all_cal_data <- files_coefficients %>%
  map_df(read_csv) 

# conversion efficiency calculation 

interp_ranges <- list(
  c("2022-09-07", "2024-02-20"),
  c("2024-04-20", "2025-04-20"),
  c("2025-06-13", "2025-10-17")
)


# Convert to POSIXct for consistent comparison
interp_ranges <- lapply(interp_ranges, function(x) as.POSIXct(x))

# --- Compute interpolated CE ---
interpolate_ce <- cal_coefficients %>%
  filter(ce_zero > 0) |> 
  filter(ce_zero < 1) |> 
  filter(cal_flag1 == 0) |>  
  filter(cal_flag2 == 0) |>  
  filter(cal_flag3 == 0) |>  
  filter(!inlet_pressure < 199) |> 
  filter(!inlet_pressure > 300) |> 
  mutate(date2 = as.Date(date)) |> 
  filter(!date2 == "2025-01-31") |> 
  filter(!date2 == "2025-01-30") |> 
  arrange(date) %>%
  mutate(
    ce_interpolated = {
      # Identify "good" points (outside all bad ranges)
      good_idx <- which(!Reduce(`|`, lapply(interp_ranges, function(r) date >= r[1] & date <= r[2])))
      
      # Perform interpolation across all times
      approx(
        x = date[good_idx],
        y = ce[good_idx],
        xout = date,
        rule = 2  # allows extrapolation if needed at boundaries
      )$y
    },
    # Keep raw CE outside bad ranges
    ce_interpolated = ifelse(
      Reduce(`|`, lapply(interp_ranges, function(r) date >= r[1] & date <= r[2])),
      ce_interpolated,  # interpolated inside bad ranges
      ce                # original outside bad ranges
    )
  )


# pressure interpolation 

pressure_correction <- cal_coefficients |> 
  filter(av_rxn_vessel_pressure < 400) |> 
  filter(ch1_sens < 10) |> 
  mutate(date = as_datetime(date, tz = "UTC")) %>% 
  mutate(period = case_when(
    date <= "2020-10-13 01:00:00" ~ "period1",
    date >= "2020-10-13 02:00:00" & date < as.Date("2021-01-11") ~ "period2",
    date >= as.Date("2021-01-11") & date < as.Date("2021-06-11") ~ "period3",
    date >= as.Date("2021-06-11") & date < as.Date("2021-12-31") ~ "period4",
    date >= as.Date("2021-12-31") & date < as.Date("2022-11-01") ~ "period5",
    date >= as.Date("2022-11-01") & date < as.Date("2023-10-19") ~ "period6",
    date >= as.Date("2023-10-19") & date < as.Date("2024-02-08") ~ "period7",
    date >= as.Date("2024-02-08") & date < as.Date("2025-02-03") ~ "period8",
    #date >= as.Date("2024-10-28") & date < as.Date("2025-02-03") ~ "period9",
    date >= as.Date("2025-02-03") & date < as.Date("2025-03-18") ~ "period9",
    date >= as.Date("2025-03-18") & date <= max(date) ~ "period10",
    TRUE ~ NA_character_  # any calibration outside periods cannot be corrected
  )) %>% 
  filter(!is.na(date)) %>% 
  mutate(year_month = floor_date(date, "month")) %>% 
  rename("reaction_cell_pressure" = av_rxn_vessel_pressure)


# get linear models for sensitivity and pressure 

lm_list_ch1 <- list(
  period1 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period1")),
  period2 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period2")),
  period3 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period3")),
  period4 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period4")),
  period5 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period5")),
  period6 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period6")),
  period7 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period7")), 
  period8 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period8")), 
  period9 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period9")), 
  period10 = lm(ch1_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period10"))
)


lm_list_ch2 <- list(
  period1 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period1")),
  period2 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period2")),
  period3 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period3")),
  period4 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period4")),
  period5 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period5")),
  period6 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period6")),
  period7 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period7")),
  period8 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period8")), 
  period9 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period9")), 
  period10 = lm(ch2_sens ~ reaction_cell_pressure, data = subset(pressure_correction, period == "period10"))
)

get_period <- function(datetime) {
  case_when(
    datetime <= "2020-10-13 01:00:00" ~ "period1",
    datetime >= "2020-10-13 02:00:00" & datetime < as.Date("2021-01-11") ~ "period2",
    datetime >= as.Date("2021-01-11") & datetime < as.Date("2021-06-11") ~ "period3",
    datetime >= as.Date("2021-06-11") & datetime < as.Date("2021-12-31") ~ "period4",
    datetime >= as.Date("2021-12-31") & datetime < as.Date("2022-11-01") ~ "period5",
    datetime >= as.Date("2022-11-01") & datetime < as.Date("2023-10-19") ~ "period6",
    datetime >= as.Date("2023-10-19") & datetime < as.Date("2024-02-08") ~ "period7",
    datetime >= as.Date("2024-02-08") & datetime < as.Date("2025-02-03") ~ "period8",
    datetime >= as.Date("2025-02-03") & datetime < as.Date("2025-03-18") ~ "period9",
    datetime >= as.Date("2025-03-18") & datetime <= max(datetime) ~ "period10",
    TRUE ~ NA_character_  
  )
}

lm_periods <- c("period1", "period3", "period4", "period7", "period8", "period10")
interp_periods <- c("period2", "period5", "period6", "period9")


# Extract coefficients for vectorized prediction
coef_tbl_ch1 <- map_dfr(names(lm_list_ch1), function(p) {
  b <- coef(lm_list_ch1[[p]])
  tibble(period = p, intercept_ch1 = b[1], slope_ch1 = b[2])
})

coef_tbl_ch2 <- map_dfr(names(lm_list_ch2), function(p) {
  b <- coef(lm_list_ch2[[p]])
  tibble(period = p, intercept_ch2 = b[1], slope_ch2 = b[2])
})


ce_interp <- interpolate_ce %>%
  select(date, ce_interpolated) %>%
  rename("ce" = ce_interpolated) |> 
  arrange(date)


  # processing the monthly param data to get 1 Hz calibration data 
  
  process_month <- function(f, out_dir = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data/") {
    message("Processing: ", f)
    
    # Read parquet for one month
    df <- open_dataset(f, format = "parquet") %>%
      collect() %>%
      mutate(datetime = parse_excel_date(TheTime)) %>%
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
        ch1_sens = case_when(
          period %in% lm_periods ~ intercept_ch1 + slope_ch1 * reaction_cell_pressure,
          period %in% interp_periods ~ NA_real_,
          TRUE ~ NA_real_
        ),
        ch2_sens = case_when(
          period %in% lm_periods ~ intercept_ch2 + slope_ch2 * reaction_cell_pressure,
          period %in% interp_periods ~ NA_real_,
          TRUE ~ NA_real_
        )
      ) |> 
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
  data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_2"
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
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  

  