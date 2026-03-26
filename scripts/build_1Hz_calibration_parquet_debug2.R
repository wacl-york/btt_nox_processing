# getting 1 Hz calibration data

# library
library(tidyverse)
library(arrow)
library(future)
library(purrr)
library(furrr)
library(waclr)
library(plotly)

# read in the 36 hour calibrations

files_coefficients <- list.files(path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/calibration_data/", 
                                 full.names = T, 
                                 pattern = "coefficients",
                                 recursive = T)


cal_coefficients_original <- files_coefficients %>%
  map_df(read_csv)


cal_coefficients_new <- cal_coefficients_original
  filter(!no_cal_flow < 5) %>% #removing any calibrations where the no cal flow went below 5 (when it is set at 10sccm)
  filter(!ch1_sens < 1) %>% #removing any calibrations where sensitiivty is wrong
  filter(!ch2_sens < 1) |> 
  filter(!date == "2025-10-17 12:00:00") %>% #dodgy cal (after instrument switched back on? multi cal day)
  filter(!date == "2021-01-14 09:00:00") %>% #inlet pressure is messed up on this cal
  filter(!date == "2021-01-12 21:00:00") #inlet pressure also messed up on this cal

# define interpolation windows - based on previous assessment of CE data 
interp_ranges <- list(
  c("2022-09-07", "2024-02-20"),
  c("2024-04-20", "2025-04-20"),
  c("2025-06-13", "2025-10-17")
) |> lapply(as.POSIXct)

# --- Compute ce_interpolated ---####
interpolate_ce_original <- cal_coefficients_original %>%
  filter(ce_zero > 0, ce_zero < 1) %>% #removing bad ce's
  arrange(date) %>%
  mutate(
    date2 = as.Date(date),
    
    # --- Step 1: mark interpolation status ---
    interp_status = ifelse(
      Reduce(`|`, lapply(interp_ranges, function(r) date >= r[1] & date <= r[2])), #if dates are within the ranges we want to interpolate, if not we keep original ce
      "yes", "no"
    )) %>% 
  filter(!(interp_status == "no" & cal_flag1 == 1)) %>% #filter out any dipped cals when we are not interpolating 
 # filter(!(interp_status == "no" & cal_flag2 == 1))|>  #filter out any no cal flow < 9.5
  filter(!(interp_status == "no" & inlet_pressure < 199)) |>  #not including ce's where inlet pressure was too low
  filter(!(interp_status == "no" & inlet_pressure > 300)) |>  #not including ce's where inlet pressure was too high
  mutate(
    # --- Step 2: compute ce_interpolated ---
    ce_interpolated = {
      ce_out <- ce_zero  # start with original
      
      for (r in interp_ranges) {
        start <- r[1]
        end <- r[2]
        
        # indices of this block
        inside_idx <- which(date >= start & date <= end)
        before_idx <- which(date < start)
        after_idx  <- which(date > end)
        
        if (length(inside_idx) == 0) next
        
        # median of last 5 valid points before block
        start_val <- if (length(before_idx) > 0) {
          median(tail(ce_zero[before_idx], 15), na.rm = TRUE)
        } else {
          ce_zero[inside_idx[1]]  # fallback
        }
        start_date <- if (length(before_idx) > 0) date[max(before_idx)] else date[inside_idx[1]]
        
        # first valid point after block
        end_val <- if (length(after_idx) > 0) ce_zero[min(after_idx)] else start_val
        end_date <- if (length(after_idx) > 0) date[min(after_idx)] else date[inside_idx[length(inside_idx)]]
        
        # interpolate linearly across the block
        ce_out[inside_idx] <- approx(
          x = as.numeric(c(start_date, end_date)),
          y = c(start_val, end_val),
          xout = as.numeric(date[inside_idx]),
          rule = 2
        )$y
      }
      
      ce_out
    }
  ) %>% 
  filter(ce_interpolated > 0.45) %>% #remove any values too low
  filter(ce_interpolated < 0.7) #remove any values too high

#new ####
interpolate_ce_new <- cal_coefficients_new %>%
  filter(ce_zero > 0, ce_zero < 1) %>% #removing bad ce's
  arrange(date) %>%
  mutate(
    date2 = as.Date(date),
    
    # --- Step 1: mark interpolation status ---
    interp_status = ifelse(
      Reduce(`|`, lapply(interp_ranges, function(r) date >= r[1] & date <= r[2])), #if dates are within the ranges we want to interpolate, if not we keep original ce
      "yes", "no"
    )) %>% 
  filter(!(interp_status == "no" & cal_flag1 == 1)) %>% #filter out any dipped cals when we are not interpolating 
  # filter(!(interp_status == "no" & cal_flag2 == 1))|>  #filter out any no cal flow < 9.5
  filter(!(interp_status == "no" & inlet_pressure < 199)) |>  #not including ce's where inlet pressure was too low
  filter(!(interp_status == "no" & inlet_pressure > 300)) |>  #not including ce's where inlet pressure was too high
  mutate(
    # --- Step 2: compute ce_interpolated ---
    ce_interpolated = {
      ce_out <- ce_zero  # start with original
      
      for (r in interp_ranges) {
        start <- r[1]
        end <- r[2]
        
        # indices of this block
        inside_idx <- which(date >= start & date <= end)
        before_idx <- which(date < start)
        after_idx  <- which(date > end)
        
        if (length(inside_idx) == 0) next
        
        # median of last 5 valid points before block
        start_val <- if (length(before_idx) > 0) {
          median(tail(ce_zero[before_idx], 15), na.rm = TRUE)
        } else {
          ce_zero[inside_idx[1]]  # fallback
        }
        start_date <- if (length(before_idx) > 0) date[max(before_idx)] else date[inside_idx[1]]
        
        # first valid point after block
        end_val <- if (length(after_idx) > 0) ce_zero[min(after_idx)] else start_val
        end_date <- if (length(after_idx) > 0) date[min(after_idx)] else date[inside_idx[length(inside_idx)]]
        
        # interpolate linearly across the block
        ce_out[inside_idx] <- approx(
          x = as.numeric(c(start_date, end_date)),
          y = c(start_val, end_val),
          xout = as.numeric(date[inside_idx]),
          rule = 2
        )$y
      }
      
      ce_out
    }
  ) %>% 
  filter(ce_interpolated > 0.45) %>% #remove any values too low
  filter(ce_interpolated < 0.7) #remove any values too high


# pressure interpolation 

pressure_correction_original <- cal_coefficients_original |> 
  filter(av_rxn_vessel_pressure < 400) |> #get rid of silly high pressure
  filter(ch1_sens < 10) |> #get rid of outlier
  mutate(date = as_datetime(date, tz = "UTC")) %>% 
  mutate(period = case_when( #here we are defining different pressure relationships that we have identified 
    date <= "2020-10-13 01:00:00" ~ "period1",
    date >= "2020-10-13 02:00:00" & date < as.Date("2021-01-11") ~ "period2",
    date >= as.Date("2021-01-11") & date < as.Date("2021-06-11") ~ "period3",
    date >= as.Date("2021-06-11") & date < as.Date("2021-12-31") ~ "period4",
    date >= as.Date("2021-12-31") & date < as.Date("2022-11-01") ~ "period5",
    date >= as.Date("2022-11-01") & date < as.Date("2023-10-19") ~ "period6",
    date >= as.Date("2023-10-19") & date < as.Date("2024-02-08") ~ "period7",
    date >= as.Date("2024-02-08") & date < as.Date("2025-02-03") ~ "period8",
    date >= as.Date("2025-02-03") & date < as.Date("2025-03-18") ~ "period9",
    date >= as.Date("2025-03-18") & date <= as.Date("2026-12-30")  ~ "period10",
    TRUE ~ NA_character_  # any calibration outside periods cannot be corrected
  )) %>% 
  filter(!is.na(date)) %>% 
  mutate(year_month = floor_date(date, "month")) 

pressure_correction_new <- cal_coefficients_new |> 
  filter(av_rxn_vessel_pressure < 400) |> #get rid of silly high pressure
  filter(ch1_sens < 10) |> #get rid of outlier
  mutate(date = as_datetime(date, tz = "UTC")) %>% 
  mutate(period = case_when( #here we are defining different pressure relationships that we have identified 
    date <= "2020-10-13 01:00:00" ~ "period1",
    date >= "2020-10-13 02:00:00" & date < as.Date("2021-01-11") ~ "period2",
    date >= as.Date("2021-01-11") & date < as.Date("2021-06-11") ~ "period3",
    date >= as.Date("2021-06-11") & date < as.Date("2021-12-31") ~ "period4",
    date >= as.Date("2021-12-31") & date < as.Date("2022-11-01") ~ "period5",
    date >= as.Date("2022-11-01") & date < as.Date("2023-10-19") ~ "period6",
    date >= as.Date("2023-10-19") & date < as.Date("2024-02-08") ~ "period7",
    date >= as.Date("2024-02-08") & date < as.Date("2025-02-03") ~ "period8",
    date >= as.Date("2025-02-03") & date < as.Date("2025-03-18") ~ "period9",
    date >= as.Date("2025-03-18") & date <= as.Date("2026-12-30")  ~ "period10",
    TRUE ~ NA_character_  # any calibration outside periods cannot be corrected
  )) %>% 
  filter(!is.na(date)) %>% 
  mutate(year_month = floor_date(date, "month")) 

# get linear models for sensitivity and pressure 

lm_list_ch1_original <- list(
  period1 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period1")),
  period2 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period2")),
  period3 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period3")),
  period4 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period4")),
  period5 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period5")),
  period6 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period6")),
  period7 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period7")), 
  period8 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period8")), 
  period9 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period9")), 
  period10 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period10"))
)

lm_list_ch1_new <- list(
  period1 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period1")),
  period2 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period2")),
  period3 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period3")),
  period4 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period4")),
  period5 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period5")),
  period6 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period6")),
  period7 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period7")), 
  period8 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period8")), 
  period9 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period9")), 
  period10 = lm(ch1_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period10"))
)


lm_list_ch2_original <- list(
  period1 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period1")),
  period2 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period2")),
  period3 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period3")),
  period4 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period4")),
  period5 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period5")),
  period6 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period6")),
  period7 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period7")),
  period8 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period8")), 
  period9 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period9")), 
  period10 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_original, period == "period10"))
)

lm_list_ch2_new <- list(
  period1 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period1")),
  period2 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period2")),
  period3 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period3")),
  period4 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period4")),
  period5 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period5")),
  period6 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period6")),
  period7 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period7")),
  period8 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period8")), 
  period9 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period9")), 
  period10 = lm(ch2_sens ~ av_rxn_vessel_pressure, data = subset(pressure_correction_new, period == "period10"))
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
    datetime >= as.Date("2025-03-18") & datetime <= as.Date("2026-12-30") ~ "period10",
    TRUE ~ NA_character_  
  )
}

lm_periods <- c("period1", "period3", "period4", "period7", "period8", "period10")
interp_periods <- c("period2", "period5", "period6", "period9") #the linear relationships are not strong enough here, instead we just linearly interpolate


# getting the pressure ranges for each linear model
pressure_ranges_original <- pressure_correction_original %>%
  group_by(period) %>%
  summarise(
    p_min = min(av_rxn_vessel_pressure, na.rm = TRUE),
    p_max = max(av_rxn_vessel_pressure, na.rm = TRUE)
  ) %>%
  ungroup()

pressure_ranges_new <- pressure_correction_new %>%
  group_by(period) %>%
  summarise(
    p_min = min(av_rxn_vessel_pressure, na.rm = TRUE),
    p_max = max(av_rxn_vessel_pressure, na.rm = TRUE)
  ) %>%
  ungroup()


# Extract coefficients for vectorized prediction
coef_tbl_ch1_original <- map_dfr(names(lm_list_ch1_original), function(p) {
  b <- coef(lm_list_ch1_original[[p]])
  tibble(period = p, intercept_ch1 = b[1], slope_ch1 = b[2])
})

coef_tbl_ch1_new <- map_dfr(names(lm_list_ch1_new), function(p) {
  b <- coef(lm_list_ch1_new[[p]])
  tibble(period = p, intercept_ch1 = b[1], slope_ch1 = b[2])
})

coef_tbl_ch2_original <- map_dfr(names(lm_list_ch2_original), function(p) {
  b <- coef(lm_list_ch2_original[[p]])
  tibble(period = p, intercept_ch2 = b[1], slope_ch2 = b[2])
})

coef_tbl_ch2_new <- map_dfr(names(lm_list_ch2_new), function(p) {
  b <- coef(lm_list_ch2_new[[p]])
  tibble(period = p, intercept_ch2 = b[1], slope_ch2 = b[2])
})

# --- merge into coefficient tables ---
coef_tbl_both_original <- coef_tbl_ch1_original %>% left_join(coef_tbl_ch2_original, by = "period")
coef_tbl_both_new <- coef_tbl_ch1_new %>% left_join(coef_tbl_ch2_new, by = "period")
coef_tbl_both_pressure_original <- coef_tbl_both_original %>% left_join(pressure_ranges_original, by = "period")
coef_tbl_both_pressure_new <- coef_tbl_both_new %>% left_join(pressure_ranges_new, by = "period")

ce_interp_original <- interpolate_ce_original %>%
  select(date, ce_interpolated) %>%
  rename("ce" = ce_interpolated) %>% 
  arrange(date)

ce_interp_new <- interpolate_ce_new %>%
  select(date, ce_interpolated) %>%
  rename("ce" = ce_interpolated) %>% 
  arrange(date)
# processing the monthly param data to get 1 Hz calibration data 

data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_2"
data_root2 <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_edit"
#out_dir <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data"

param_files <- list.files(file.path(data_root),
                          full.names = T, 
                          pattern = "\\.parquet$",
                          recursive = T)

param_files_new <- list.files(file.path(data_root2),
                          full.names = T, 
                          pattern = "\\.parquet$",
                          recursive = T)

n_files <- length(param_files)

i = 22
  
  for (i in seq_along(param_files)){
    f<- param_files[i]
    g<- param_files_new[i]
    message(sprintf("[%d/%d] Processing: %s", i, n_files, basename(f)))
    
    # Read parquet for one month
    df_original <- open_dataset(f, format = "parquet") %>%
      collect() %>%
      mutate(datetime = parse_excel_date(TheTime, tz = "UTC")) %>%
      rename_all(tolower) %>% 
      rename("av_rxn_vessel_pressure" = rxn_vessel_pressure) %>% 
      arrange(datetime)
    
    df_new <- open_dataset(g, format = "parquet") %>%
      collect() %>%
      mutate(datetime = parse_excel_date(TheTime, tz = "UTC")) %>%
      rename_all(tolower) %>% 
      rename("av_rxn_vessel_pressure" = rxn_vessel_pressure) %>% 
      arrange(datetime)
    
    
    if (nrow(df) == 0) {
      message("Skipping empty file: ", basename(f))
      next
    }
    
    # --- interpolate CE for this month only
    df_original <- df_original %>%
      mutate(
        ce = approx(ce_interp_original$date, ce_interp_original$ce, xout = datetime, rule = 2)$y
      )
    
    df_new <- df_new %>%
      mutate(
        ce = approx(ce_interp_new$date, ce_interp_new$ce, xout = datetime, rule = 2)$y
      )
    
    
    # --- add period
    df_original <- df_original %>%
      mutate(period = get_period(datetime))
    
    df_new <- df_new %>%
      mutate(period = get_period(datetime))
    
    # --- join model coefficients
    df_original <- df_original %>%
      left_join(coef_tbl_both_pressure_original, by = "period") %>%
      arrange(datetime) %>%
      mutate(
        ch1_sens_raw = case_when(
          period %in% lm_periods &
            av_rxn_vessel_pressure <= 40 &
            av_rxn_vessel_pressure >= p_min &
            av_rxn_vessel_pressure <= p_max ~
            intercept_ch1 + slope_ch1 * av_rxn_vessel_pressure,
          TRUE ~ NA_real_
        ),
        ch2_sens_raw = case_when(
          period %in% lm_periods &
            av_rxn_vessel_pressure <= 40 &
            av_rxn_vessel_pressure >= p_min &
            av_rxn_vessel_pressure <= p_max ~
            intercept_ch2 + slope_ch2 * av_rxn_vessel_pressure,
          TRUE ~ NA_real_
        ),
        ch1_sens_coeff = approx(cal_coefficients_original$date, cal_coefficients_original$ch1_sens, xout = datetime, rule = 2)$y, 
        ch2_sens_coeff = approx(cal_coefficients_original$date, cal_coefficients_original$ch2_sens, xout = datetime, rule = 2)$y, 
        
        # --- Final sensitivities ---
        # If the pressure model has a valid value, use it
        # Otherwise, fall back to the time-interpolated calibration sensitivity
        ch1_sens = ifelse(!is.na(ch1_sens_raw), ch1_sens_raw, ch1_sens_coeff),
        ch2_sens = ifelse(!is.na(ch2_sens_raw), ch2_sens_raw, ch2_sens_coeff)
      ) %>% 
      
      # --- Apply final physical filters ---
      mutate(
        ch1_sens = ifelse(
          av_rxn_vessel_pressure > 40 |
            inlet_pressure < 199 |
            inlet_pressure > 350,
          NA_real_, ch1_sens
        ),
        ch2_sens = ifelse(
          av_rxn_vessel_pressure > 40 |
            inlet_pressure < 199 |
            inlet_pressure > 350,
          NA_real_, ch2_sens
        ),
        ce = ifelse(
          av_rxn_vessel_pressure > 40 |
            inlet_pressure < 199 |
            inlet_pressure > 350,
          NA_real_, ce
        )
      ) %>%
      
      select(
        -intercept_ch1, -slope_ch1,
        -intercept_ch2, -slope_ch2,
        -p_min, -p_max
      )
    
    df_new <- df_new %>%
      left_join(coef_tbl_both_pressure_new, by = "period") %>%
      arrange(datetime) %>%
      mutate(
        ch1_sens_raw = case_when(
          period %in% lm_periods &
            av_rxn_vessel_pressure <= 40 &
            av_rxn_vessel_pressure >= p_min &
            av_rxn_vessel_pressure <= p_max ~
            intercept_ch1 + slope_ch1 * av_rxn_vessel_pressure,
          TRUE ~ NA_real_
        ),
        ch2_sens_raw = case_when(
          period %in% lm_periods &
            av_rxn_vessel_pressure <= 40 &
            av_rxn_vessel_pressure >= p_min &
            av_rxn_vessel_pressure <= p_max ~
            intercept_ch2 + slope_ch2 * av_rxn_vessel_pressure,
          TRUE ~ NA_real_
        ),
        ch1_sens_coeff = approx(cal_coefficients_new$date, cal_coefficients_new$ch1_sens, xout = datetime, rule = 2)$y, 
        ch2_sens_coeff = approx(cal_coefficients_new$date, cal_coefficients_new$ch2_sens, xout = datetime, rule = 2)$y, 
        
        # --- Final sensitivities ---
        # If the pressure model has a valid value, use it
        # Otherwise, fall back to the time-interpolated calibration sensitivity
        ch1_sens = ifelse(!is.na(ch1_sens_raw), ch1_sens_raw, ch1_sens_coeff),
        ch2_sens = ifelse(!is.na(ch2_sens_raw), ch2_sens_raw, ch2_sens_coeff)
      ) %>% 
      
      # --- Apply final physical filters ---
      mutate(
        ch1_sens = ifelse(
          av_rxn_vessel_pressure > 40 |
            inlet_pressure < 199 |
            inlet_pressure > 350,
          NA_real_, ch1_sens
        ),
        ch2_sens = ifelse(
          av_rxn_vessel_pressure > 40 |
            inlet_pressure < 199 |
            inlet_pressure > 350,
          NA_real_, ch2_sens
        ),
        ce = ifelse(
          av_rxn_vessel_pressure > 40 |
            inlet_pressure < 199 |
            inlet_pressure > 350,
          NA_real_, ce
        )
      ) %>%
      
      select(
        -intercept_ch1, -slope_ch1,
        -intercept_ch2, -slope_ch2,
        -p_min, -p_max
      )
    
    zero_data_original <- df_original %>%
      filter(zero_valve_1 == 1.0, zero_valve_2 == 1.0) %>%
      mutate(second = second(datetime), 
             hour_block = floor_date(datetime, "hour")) %>%
      filter(between(second, 5, 15)) %>%
      group_by(hour_block) %>% # we only want the zeros from the start of the hour (when we do a cal there are multiple zeros)
      summarise(
        date = min(datetime),
        ch1_hz = median(ch1_hz, na.rm = TRUE), #take the median of the zero and interpolate that 
        ch2_hz = median(ch2_hz, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(date)
    
    if (nrow(zero_data_original) > 0) {
      df_original <- df_original %>%
        mutate(
          ch1_zero = approx(zero_data_original$date, zero_data_original$ch1_hz, xout = datetime, rule = 2)$y,
          ch2_zero = approx(zero_data_original$date, zero_data_original$ch2_hz, xout = datetime, rule = 2)$y
        )
    } else {
      df_original <- df_original %>%
        mutate(ch1_zero = NA_real_, ch2_zero = NA_real_)
    }
    
    
    zero_data_new <- df_new %>%
      filter(zero_valve_1 == 1.0, zero_valve_2 == 1.0) %>%
      mutate(second = second(datetime), 
             hour_block = floor_date(datetime, "hour")) %>%
      filter(between(second, 5, 15)) %>%
      group_by(hour_block) %>% # we only want the zeros from the start of the hour (when we do a cal there are multiple zeros)
      summarise(
        date = min(datetime),
        ch1_hz = median(ch1_hz, na.rm = TRUE), #take the median of the zero and interpolate that 
        ch2_hz = median(ch2_hz, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(date)
    
    if (nrow(zero_data_new) > 0) {
      df_new <- df_new %>%
        mutate(
          ch1_zero = approx(zero_data_new$date, zero_data_new$ch1_hz, xout = datetime, rule = 2)$y,
          ch2_zero = approx(zero_data_new$date, zero_data_new$ch2_hz, xout = datetime, rule = 2)$y
        )
    } else {
      df_new <- df_new %>%
        mutate(ch1_zero = NA_real_, ch2_zero = NA_real_)
    }
    
    df_new <- df_new %>% 
      select(c(ch1_hz_new = ch1_hz, ch2_hz_new = ch2_hz, ch1_sens_new = ch1_sens, 
               ch2_sens_new = ch2_sens, ch1_zero_new = ch1_zero, ch2_zero_new = ch2_zero, ce_new = ce, 
               datetime))
    
    
    df_original <- df_original %>% 
      select(c(ch1_hz_original = ch1_hz, ch2_hz_original = ch2_hz, ch1_sens_original = ch1_sens, 
               ch2_sens_original = ch2_sens, ch1_zero_original = ch1_zero, ch2_zero_original = ch2_zero, ce_original = ce, 
               datetime))
    
    
    join_both <- left_join(df_original, df_new, by = "datetime") %>% 
      mutate(count = row_number())
    
    
    join_both %>% 
      filter(count > 1000) %>% 
      filter(count < 4000) %>% 
      pivot_longer(cols = c(ce_original, ce_new), 
                   names_to = "type", values_to = "value") %>% 
      ggplot(aes(count, value, color = type)) +
      geom_line() +
      facet_wrap(~type, ncol = 1)
    
    ggplotly()
    
    
    # --- figure out year for subfolder
    year_val <- year(min(df$datetime, na.rm = TRUE))
    year_dir <- file.path(out_dir, as.character(year_val))
    dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
    
    # --- write calibrated parquet into year subfolder
    out_file <- file.path(year_dir, basename(f))
    write_parquet(df, out_file)
    
    message("Saved: ", out_file)
  }
  
  # # -------------------------
  # # 4. Run over all files (parallel)
  # # -------------------------
  # data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_2"
  # param_files <- list.files(file.path(data_root),
  #                           full.names = T, 
  #                           pattern = "\\.parquet$",
  #                           recursive = T)
  # 
  # plan(multisession, workers = 4)  # adjust cores
  # future_walk(param_files, process_month)
  # 
  # 
  # 
  # 
  # # checking
  file_path <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data/2020/param_2020_10.parquet"   
  
# # Read the file
  param_data <- arrow::read_parquet(file_path)
  # 
  # # Quick overview of the dataset
   collect(param_data) 
  
  
  ggplot(param_data, aes(datetime, ch2_sens))+
    geom_line()
  
  
  
  
  
  
  
  
  
  
  

  