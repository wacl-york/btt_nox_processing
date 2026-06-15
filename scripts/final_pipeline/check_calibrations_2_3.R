# library

library(tidyverse)
library(plotly)


# check cals 

files_coefficients <- list.files(path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/calibration_data/", 
                                 full.names = T, 
                                 pattern = "coefficients",
                                 recursive = T)


cal_coefficients <- files_coefficients %>%
  map_df(read_csv) %>% 
  filter(!no_cal_flow < 5) %>% #removing any calibrations where the no cal flow went below 5 (when it is set at 10sccm)
  filter(!ch1_sens < 1) %>% #removing any calibrations where sensitiivty is wrong
  filter(!ch2_sens < 1) |> 
  filter(ce < 1) %>% 
  filter(ce > 0) %>% 
  filter(!date == "2025-10-17 12:00:00") %>% #dodgy cal (after instrument switched back on? multi cal day)
  filter(!date == "2021-01-14 09:00:00") %>% #inlet pressure is messed up on this cal
  filter(!date == "2021-01-12 21:00:00")


# check ce 

cal_coefficients %>% 
  mutate(year = year(date)) %>% 
  ggplot(aes(date, ce)) + 
  geom_line()+
  theme_bw()

ggplotly()


interp_ranges <- list(
  c("2022-09-07", "2024-02-20"),
  c("2024-04-20", "2025-04-20"),
  c("2025-06-13", "2025-10-17"),
  c("2026-01-02", "2026-03-01")
) |> lapply(as.POSIXct)

# --- Compute ce_interpolated ---
interpolate_ce <- cal_coefficients %>%
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

ggplot(interpolate_ce, aes(date, ce_interpolated))+
  geom_line()

ggplotly()

# check sensitivity

cal_coefficients %>% 
  mutate(year = year(date)) %>% 
  # filter(date >= "2024-12-01 00:00:00") %>% 
  # filter(date <= "2026-03-30 00:00:00") %>% 
  rename("Channel 1" = )
  pivot_longer(cols = c(ch1_sens, ch2_sens), 
               names_to = "channel", values_to = "sens") %>% 
  ggplot(aes(date, sens, color = channel)) +
  geom_line()+
  theme_bw()

ggplotly()

cal_coefficients %>% 
  mutate(year = year(date)) %>% 
  filter(date >= "2024-12-01 00:00:00") %>% 
  filter(date <= "2026-03-30 00:00:00") %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens), 
               names_to = "channel", values_to = "sens") %>% 
  ggplot(aes(date, av_rxn_vessel_pressure, color = channel)) +
  geom_line() +
  theme_bw()


files_sens_params <- list.files(path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/calibration_data/", 
                                 full.names = T, 
                                 pattern = "sensitivity_params",
                                 recursive = T)


sens_params <- files_sens_params %>%
  map_df(read_csv) %>% 
  filter(!date == "2025-10-17 12:00:00") %>% #dodgy cal (after instrument switched back on? multi cal day)
  filter(!date == "2021-01-14 09:00:00") %>% #inlet pressure is messed up on this cal
  filter(!date == "2021-01-12 21:00:00")


# check pressure

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
    date >= as.Date("2024-02-08") & date < as.Date("2024-08-31") ~ "period8",
    date >= as.Date("2024-08-31") & date < as.Date("2025-03-18") ~ "period9", #needs to be interpolated due to bad inlet pressure
    date >= as.Date("2025-03-18") & date < as.Date("2025-06-05")  ~ "period10",
    date >= as.Date("2025-06-05") & date < as.Date("2025-11-13")  ~ "period11",
    date >= as.Date("2025-11-13") & date < as.Date("2025-12-30")  ~ "period12",
    date >= as.Date("2025-12-30") & date <= as.Date("2026-12-30")  ~ "period13",
    TRUE ~ NA_character_  # any calibration outside periods cannot be corrected
  )) %>% 
  filter(!is.na(date)) %>% 
  mutate(year_month = floor_date(date, "month")) 

ggplot(pressure_correction, aes(date, av_rxn_vessel_pressure, color = period))+
  geom_line()

ggplotly()

pressure_correction %>% 
  pivot_longer(cols = c(ch1_sens, ch2_sens), names_to = "channel", values_to = "sens") %>% 
ggplot(aes(date, sens, color = period))+
  geom_line()+
  facet_wrap(~channel)


ggplot(pressure_correction, aes(av_rxn_vessel_pressure, ch1_sens, color = period)) +
  geom_line() +
  facet_wrap(~period, scales = "free")+
  theme_minimal()



# want to check inlet pressure 

ggplot(pressure_correction, aes(date, inlet_pressure, color = period))+
  geom_line()

ggplotly()

