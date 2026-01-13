# library

library(tidyverse)
library(purrr)
library(furrr)
library(arrow)
library(waclr)


# reading in the raw calibration data


files_coefficients <- list.files(path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/calibration_data/", 
                                 full.names = T, 
                                 pattern = "coefficients",
                                 recursive = T)


cal_coefficients <- files_coefficients %>%
  map_df(read_csv) 


cal_coefficients %>% 
  filter(av_rxn_vessel_pressure < 400) |> #get rid of silly high pressure
  filter(ch1_sens < 10) %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens), 
               names_to = "channel", values_to = "sensitivity") %>% 
  ggplot(aes(date, sensitivity, color = channel)) +
  geom_line() +
  theme_bw()


pressure_correction <- cal_coefficients |> 
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
    date >= as.Date("2025-03-18") & date <= max(date) ~ "period10",
    TRUE ~ NA_character_  # any calibration outside periods cannot be corrected
  )) %>% 
  filter(!is.na(date)) %>% 
  mutate(year_month = floor_date(date, "month")) 

pressure_correction %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens), 
               names_to = "channel", values_to = "sensitivity") %>% 
  ggplot(aes(av_rxn_vessel_pressure, sensitivity, color = period)) +
  geom_line()+
  facet_wrap(period~channel, scales = "free") +
  theme_bw()


pressure_correction %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens), 
               names_to = "channel", values_to = "sensitivity") %>% 
  ggplot(aes(date, sensitivity, color = period)) +
  geom_line()+
  facet_wrap(~channel) +
  theme_bw()



data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_2"
out_dir <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data"

param_files <- list.files(file.path(data_root),
                          full.names = T, 
                          pattern = "\\.parquet$",
                          recursive = T)

n_files <- length(param_files)
i = 29
#for (i in seq_along(param_files)){
  f<- param_files[i]
  message(sprintf("[%d/%d] Processing: %s", i, n_files, basename(f)))
  
  # Read parquet for one month
  df <- open_dataset(f, format = "parquet") %>%
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
  df <- df %>%
    mutate(
      ce = approx(ce_interp$date, ce_interp$ce, xout = datetime, rule = 2)$y
    )
  
  # --- add period
  df <- df %>%
    mutate(period = get_period(datetime))
  
  # --- join model coefficients
  df <- df %>%
    left_join(coef_tbl_both_pressure, by = "period") %>%
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
      ch1_sens_coeff = approx(cal_coefficients$date, cal_coefficients$ch1_sens, xout = datetime, rule = 2)$y, 
      ch2_sens_coeff = approx(cal_coefficients$date, cal_coefficients$ch2_sens, xout = datetime, rule = 2)$y, 
      
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
  
  
df %>% 
  filter(datetime > "2023-12-04 09:00:00") %>% 
  filter(datetime < "2023-12-04 09:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 7")


df %>% 
  filter(datetime > "2020-10-10 09:00:00") %>% 
  filter(datetime < "2020-10-10 09:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 1")


df %>% 
  filter(datetime > "2021-01-03 09:00:00") %>% 
  filter(datetime < "2021-01-03 09:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 2")

df %>% 
  filter(datetime > "2021-05-05 10:00:00") %>% 
  filter(datetime < "2021-05-05 10:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 3")

df %>% 
  filter(datetime > "2021-08-05 10:00:00") %>% 
  filter(datetime < "2021-08-05 10:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 4")

df %>% 
  filter(datetime > "2022-04-05 10:00:00") %>% 
  filter(datetime < "2022-04-05 10:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 5")

df %>% 
  filter(datetime > "2023-06-05 10:00:00") %>% 
  filter(datetime < "2023-06-05 10:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 6")

df %>% 
  filter(datetime > "2024-06-05 10:00:00") %>% 
  filter(datetime < "2024-06-05 10:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 8")


df %>% 
  filter(datetime > "2025-02-20 10:00:00") %>% 
  filter(datetime < "2025-02-20 10:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 9")

df %>% 
  filter(datetime > "2025-10-20 10:00:00") %>% 
  filter(datetime < "2025-10-20 10:05:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()+
  labs(title = "Period 10")

check <- df %>% 
  filter(datetime > "2022-06-14 00:00:00") %>% 
  filter(datetime < "2022-06-15 00:05:00") 
  pivot_longer(cols = c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), 
               names_to = "type", values_to = "value") %>% 
  ggplot(aes(datetime, value, color = type)) +
  geom_line() +
  facet_wrap(~type, scales = "free_y")+
  theme_bw()

  
  zero_data <- df %>%
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
  
  zero_data_29 <- zero_data
  
  all_zero <- rbind(zero_data_29, zero_data_30, zero_data_31, zero_data_32, zero_data_33, zero_data_34, zero_data_35, zero_data_36, 
                    zero_data_37, zero_data_38, zero_data_39, zero_data_40)
  
  all_zero %>% 
    pivot_longer(cols = c(ch1_hz, ch2_hz), 
                 names_to = "channel", values_to = "counts") %>% 
  ggplot(aes(date, counts, color = channel)) +
    geom_line(alpha = 0.5) +
    theme_bw() +
    facet_wrap(~channel) +
    labs(x = "Date", y = "Zero counts")
  
  
  if (nrow(zero_data) > 0) {
    df <- df %>%
      mutate(
        ch1_zero = approx(zero_data$date, zero_data$ch1_hz, xout = datetime, rule = 2)$y,
        ch2_zero = approx(zero_data$date, zero_data$ch2_hz, xout = datetime, rule = 2)$y
      )
  } else {
    df <- df %>%
      mutate(ch1_zero = NA_real_, ch2_zero = NA_real_)
  }
  
  df %>% 
    filter(datetime > "2022-06-14 00:00:00") %>% 
    filter(datetime < "2022-06-15 00:05:00") %>% 
  pivot_longer(cols = c(ch1_zero, ch2_zero), 
               names_to = "type", values_to = "value") %>% 
    ggplot(aes(datetime, value, color = type)) +
    geom_line() +
    facet_wrap(~type, scales = "free_y")+
    theme_bw()
  
  



# --- figure out year for subfolder
  year_val <- year(min(df$datetime, na.rm = TRUE))
  year_dir <- file.path(out_dir, as.character(year_val))
  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
  
  # --- write calibrated parquet into year subfolder
  out_file <- file.path(year_dir, basename(f))
  write_parquet(df, out_file)
  
  message("Saved: ", out_file)
#}

