
library(readr)
library(dplyr)
library(lubridate)
library(arrow)
library(waclr)

# Load calibration times
all_cal_times <- read_csv("data/data/cal_times_all.csv") |> 
  mutate(date2 = as.Date(date)) |> 
  filter(!(date2 %in% c("2023-03-30", "2023-04-29", "2024-04-11", "2024-04-12", "2024-05-09", "2023-08-08", "2025-02-01",
                        "2025-02-02"))) |>
  filter(!(date >= as.Date("2025-02-22") & date <= as.Date("2025-03-04"))) |> 
  select(date) 

cal_file <- all_cal_times |>
  arrange(date)

# List all Parquet parameter files
param_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params/"

param_files <- list.files(file.path(param_root), 
                          full.names = TRUE, 
                          pattern = "param_\\d{4}_\\d{2}\\.parquet$", 
                          recursive = TRUE)

# Extract year and month from filenames
param_info <- data.frame(
  file = param_files,
  year = as.integer(sub(".*param_(\\d{4})_\\d{2}\\.parquet", "\\1", basename(param_files))),
  month = as.integer(sub(".*param_\\d{4}_(\\d{2})\\.parquet", "\\1", basename(param_files)))
)

# Define expected columns for param_data
expected_param_columns <- c(
  "date", "ch1_hz", "ch2_hz", "no_cal_flow", "no_cal_flow_set", "sampleflow1", "sampleflow2",
  "blc_lamp_1", "zero_valve_1", "zero_valve_2", "nox_cal", "zero_trap", 
  "blc_temp", "control_temp", "rxn_vessel_pressure", "count"
)

# Initialize calibration coefficients table
cal_coefficients <- data.frame(matrix(ncol = 15, nrow = 0))
colnames(cal_coefficients) <- c(
  "date", "ch1_sens", "ch2_sens", "ce", "ce_eq2", "no_cal_flow", 
  "mean_ch1_sd", "mean_ch2_sd", "av_control_temp", "av_rxn_vessel_pressure",
  "cal_flag1", "cal_flag2", "cal_flag3", "ce_flag1", "ce_flag2"
)

cal_process <- data.frame(cal_time = numeric(), process = numeric())
all_cal_data <- data.frame()
all_con_eff <- data.frame()
param_all <- data.frame()

cal_times <- cal_file$date

for (i in 1:length(cal_times)) {
  
  cal_timestamp <- floor_date(cal_times[i], "hour")
  cal_year <- year(cal_timestamp)
  cal_month <- sprintf("%02d", month(cal_timestamp))
  
  cat(paste("Processing calibration", i, "of", length(cal_times), "at", cal_timestamp, "\n"))
  
  # Find the Parquet file for the calibration's year and month
  param_file <- param_info$file[param_info$year == cal_year & param_info$month == as.integer(cal_month)]
  
  if (length(param_file) == 0) {
    warning(paste("No parameter file found for", cal_timestamp))
    next
  }
  
  # Read and filter the Parquet file for the specific hour
  param_data <- read_parquet(param_file) %>%
    mutate(date = parse_excel_date(TheTime)) %>%
    rename_all(tolower) %>%
    filter(date >= floor_date(cal_timestamp, "hour"),
           date < floor_date(cal_timestamp, "hour") + 3600,
           ch1_hz > 0,
           ch2_hz > 0) %>%
    mutate(count = row_number()) %>%
    select(any_of(expected_param_columns))
  
  if (nrow(param_data) == 0) {
    warning(paste("No data found in Parquet file for", cal_timestamp))
    next
  }
  
  # Debug: Print column names if mismatch is suspected
  if (i == 1 || nrow(param_all) > 0) {
    cat("Columns in param_data:", colnames(param_data), "\n")
    if (nrow(param_all) > 0) {
      cat("Columns in param_all:", colnames(param_all), "\n")
    }
  }
  
  # Use bind_rows to handle column mismatches
  param_all <- bind_rows(param_all, param_data)
  
  #  if (sum(unique(param_data$no_cal_flow_set), na.rm = TRUE) < 50) next # Skip incomplete calibration
  
  ten <- param_data %>% 
    filter(no_cal_flow_set == 10,
           blc_lamp_1 == 0,
           zero_valve_1 == 0,
           nox_cal == 0) 
  
  av_BLC_temp <- mean(ten$blc_temp, na.rm = TRUE)
  av_control_temp <- mean(ten$control_temp, na.rm = TRUE)
  no_cal_flow <- mean(ten$no_cal_flow, na.rm = TRUE)
  av_rxn_vessel_pressure <- mean(ten$rxn_vessel_pressure, na.rm = TRUE)
  
  # if (nrow(ten) < 170) {
  #   print(paste0("Skipping: ", i))
  #   next
  # }
  
  ten_multi <- ten %>%
    mutate(idx = row_number()) |>
    filter(idx > 120)
  
  eighteen <- param_data %>% 
    filter(no_cal_flow_set == 18) %>%
    mutate(idx = row_number()) |>
    filter(idx > 300)
  
  five <- param_data %>% 
    filter(no_cal_flow_set == 5) %>% 
    mutate(idx = row_number()) |>
    filter(idx > 300)
  
  fifteen <- param_data %>% 
    filter(no_cal_flow_set == 15) %>% 
    mutate(idx = row_number()) |>
    filter(idx > 300)
  
  two.five <- param_data %>% 
    filter(no_cal_flow_set == 2.5) %>% 
    mutate(idx = row_number()) |>
    filter(idx > 200) |> 
    filter(idx < 600)
  
  zero_data <- param_data |>
    filter(zero_valve_2 == 1.0, 
           zero_valve_1 == 1.0) |>
    mutate(second = second(date)) |> 
    filter(between(second, 5, 15)) |>
    select(date, ch1_hz, ch2_hz) |>
    mutate(date = floor_date(date, unit = "second"))
  
  zero_ch1 <- as.numeric(median(zero_data$ch1_hz, na.rm = TRUE))
  zero_ch2 <- as.numeric(median(zero_data$ch2_hz, na.rm = TRUE))
  
  zero_data_median <- zero_data |> 
    mutate(date2 = as.Date(date)) |> 
    group_by(date2) |> 
    summarise(zero_ch1 = median(ch1_hz, na.rm = TRUE), 
              zero_ch2 = median(ch2_hz, na.rm = TRUE))
  
  
  # Collect unique calibration flows present in the data
  flows_present <- unique(param_data$no_cal_flow_set)
  
  # Define flow sets
  required_multi <- c(2.5, 5, 10, 15, 18)
  multi_others   <- c(2.5, 5, 15, 18)
  single_flow    <- 10
  
  if (all(required_multi %in% flows_present)) {
    # ---------------------
    # Multi-point calibration
    # ---------------------
    cal_data <- bind_rows(ten_multi, eighteen, five, fifteen, two.five) %>%
      dplyr::select(date, ch1_hz, ch2_hz, no_cal_flow, sampleflow1, sampleflow2, no_cal_flow_set) %>%
      group_by(no_cal_flow_set) %>%
      summarise_all(mean, na.rm = TRUE) %>%
      ungroup() %>%
      mutate(
        cyl_conc = case_when(
          date < ymd_hms("2020-12-18 00:00:00") ~ 5.03e6,
          date > ymd_hms("2020-12-18 00:00:00") & date < ymd_hms("2021-03-29 00:00:00") ~ 4.96e6,
          date > ymd_hms("2021-03-29 00:00:00") ~ 5.1e6
        ),
        total_sample_flow = sampleflow1 + sampleflow2,
        cal_conc = cyl_conc * (no_cal_flow / total_sample_flow),
        sens_type = "multi"
      )
    
    if (nrow(cal_data) < 2) {  # need at least 2 points to fit a line
      warning(paste("Not enough data to fit calibration for", cal_timestamp))
      next  # skip this calibration
    }
    cal_slope_ch1 <- lm(ch1_hz ~ cal_conc, data = cal_data)$coefficients[2]
    cal_slope_ch2 <- lm(ch2_hz ~ cal_conc, data = cal_data)$coefficients[2]
    
    cal_data <- cal_data %>%
      mutate(
        sens_ch1 = cal_slope_ch1,
        sens_ch2 = cal_slope_ch2,
        date = as.Date(date)
      )
    
  } else if (single_flow %in% flows_present && !any(multi_others %in% flows_present)) {
    # ---------------------
    # Single-point calibration (10 sccm ONLY)
    # ---------------------
    ten_single <- ten %>%
      mutate(idx = row_number()) |>
      filter(idx > 20)  
    
    cyl_conc <- case_when(
      max(ten_single$date, na.rm = TRUE) < ymd_hms("2020-12-18 00:00:00") ~ 5.03e6,
      max(ten_single$date, na.rm = TRUE) < ymd_hms("2021-03-29 00:00:00") ~ 4.96e6,
      TRUE ~ 5.1e6
    )
    
    total_sample_flow <- mean(ten_single$sampleflow1, na.rm = TRUE) + 
      mean(ten_single$sampleflow2, na.rm = TRUE)
    
    cal_conc <- cyl_conc * (mean(ten_single$no_cal_flow, na.rm = TRUE) / total_sample_flow)
    
    cal_slope_ch1 <- ((median(ten_single$ch1_hz, na.rm = TRUE)) - zero_ch1) / cal_conc
    cal_slope_ch2 <- ((median(ten_single$ch2_hz, na.rm = TRUE)) - zero_ch2) / cal_conc
    
    cal_data <- tibble(
      no_cal_flow_set = 10,
      ch1_hz = mean(ten_single$ch1_hz, na.rm = TRUE),
      ch2_hz = mean(ten_single$ch2_hz, na.rm = TRUE),
      no_cal_flow = mean(ten_single$no_cal_flow, na.rm = TRUE),
      sampleflow1 = mean(ten_single$sampleflow1, na.rm = TRUE),
      sampleflow2 = mean(ten_single$sampleflow2, na.rm = TRUE),
      cyl_conc = cyl_conc,
      total_sample_flow = total_sample_flow,
      cal_conc = cal_conc,
      sens_ch1 = cal_slope_ch1,
      sens_ch2 = cal_slope_ch2,
      date = as.Date(cal_timestamp),
      sens_type = "single"
    )
    
  } else {
    # ---------------------
    # No valid calibration
    # ---------------------
    message("  skipping: calibration is neither full multi-point nor single-10 only")
    next
  }
  
  
  all_cal_data <- bind_rows(all_cal_data, cal_data)
  
  # ---------------------
  # Conversion efficiency calculation
  # ---------------------
  
  if (all(required_multi %in% flows_present)) {
    # ---------------------
    # Multi-point CE stages
    # ---------------------
    gpt_0_blc_1 <- param_data %>% 
      filter(no_cal_flow_set == 10,
             blc_lamp_1 == 1,
             zero_valve_1 == 0,
             nox_cal == 0) %>% 
      filter(count > 861, count < 975) %>%
      mutate(type = "gpt_0_blc_1") # STAGE 4
    
    gpt_1_blc_1 <- param_data %>% 
      filter(no_cal_flow_set == 10,
             blc_lamp_1 == 1,
             zero_valve_1 == 0,
             nox_cal == 1) %>% 
      filter(count > 440, count < 600) %>%
      mutate(type = "gpt_1_blc_1") # STAGE 2
    
    gpt_0_blc_0 <- param_data %>% 
      filter(no_cal_flow_set == 10,
             blc_lamp_1 == 0,
             zero_valve_1 == 0,
             nox_cal == 0) %>% 
      filter(count > 683, count < 800) %>%
      mutate(type = "gpt_0_blc_0") # STAGE 3
    
    gpt_1_blc_0 <- param_data %>% 
      filter(no_cal_flow_set == 10,
             blc_lamp_1 == 0,
             zero_valve_1 == 0,
             nox_cal == 1) %>% 
      filter(count > 316, count < 416) %>%
      mutate(type = "gpt_1_blc_0") # STAGE 1
    
  } else if (single_flow %in% flows_present && !any(multi_others %in% flows_present)) {
    # ---------------------
    # Single-point CE stages
    # ---------------------
    gpt_0_blc_1 <- param_data %>% 
      filter(no_cal_flow_set == 10,
             blc_lamp_1 == 1,
             zero_valve_1 == 0,
             nox_cal == 0) %>% 
      filter(count > 600, count < 675) %>%
      mutate(type = "gpt_0_blc_1")  # STAGE 4
    
    gpt_1_blc_1 <- param_data %>% 
      filter(no_cal_flow_set == 10,
             blc_lamp_1 == 1,
             zero_valve_1 == 0,
             nox_cal == 1) %>% 
      filter(count > 350, count < 420) %>%
      mutate(type = "gpt_1_blc_1")  # STAGE 2
    
    gpt_0_blc_0 <- param_data %>% 
      filter(no_cal_flow_set == 10,
             blc_lamp_1 == 0,
             zero_valve_1 == 0,
             nox_cal == 0) %>% 
      filter(count > 475, count < 550) %>%
      mutate(type = "gpt_0_blc_0")  # STAGE 3
    
    gpt_1_blc_0 <- param_data %>% 
      filter(no_cal_flow_set == 10,
             blc_lamp_1 == 0,
             zero_valve_1 == 0,
             nox_cal == 1) %>% 
      filter(count > 250, count < 300) %>%
      mutate(type = "gpt_1_blc_0")  # STAGE 1
    
  } else {
    # ---------------------
    # Not a valid calibration
    # ---------------------
    message("Skipping CE calc: neither full multi-point nor single-10 only")
    next
  }
  
  # Zero calibration check (applies to both modes)
  zero_gpt_0_blc_1 <- param_data |> 
    filter(no_cal_flow_set == 10, 
           zero_trap == 1, 
           zero_valve_2 == 1.0, 
           zero_valve_1 == 1.0)
  
  if (nrow(gpt_0_blc_0) == 0) {
    cat("No obs for gpt_0_blc_0 at calibration", i, "\n")
    next
  }
  
  if (nrow(gpt_1_blc_0) == 0) {
    cat("No obs for gpt_1_blc_0 at calibration", i, "\n")
    next
    
  }
  
  if (nrow(gpt_1_blc_1) == 0) {
    cat("No obs for gpt_1_blc_1 at calibration", i, "\n")
    next
  }
  
  if (nrow(gpt_0_blc_1) == 0) {
    cat("No obs for gpt_0_blc_1 at calibration", i, "\n")
    next
  }
  
  cal_flag1 <- ifelse(mean(gpt_1_blc_0$ch1_hz, na.rm = TRUE) - mean(gpt_1_blc_1$ch1_hz, na.rm = TRUE) > 5000, 1, 0)
  
  con_eff <- bind_rows(gpt_0_blc_1,
                       gpt_1_blc_1,
                       gpt_0_blc_0,
                       gpt_1_blc_0) %>% 
    group_by(type) %>% 
    summarise_all(median, na.rm = TRUE)  %>% 
    dplyr::select(type, ch2_hz) %>% 
    pivot_wider(names_from = type,
                values_from = ch2_hz) %>%
    mutate(ce = 1 - ((gpt_0_blc_1 - gpt_1_blc_1) / (gpt_0_blc_0 - gpt_1_blc_0)))
  
  ce <- con_eff$ce 
  
  all_stages <- bind_rows(gpt_0_blc_1,
                          gpt_1_blc_1,
                          gpt_0_blc_0,
                          gpt_1_blc_0) %>%
    mutate(date = as.Date(date)) |> 
    group_by(type, date) %>% 
    summarise(
      median_ch2 = median(ch2_hz, na.rm = TRUE),
      mean_flow = mean(no_cal_flow, na.rm = TRUE), 
      sampleflow1 = median(sampleflow1, na.rm = TRUE), 
      sampleflow2 = median(sampleflow2, na.rm = TRUE), 
      zero_ch1 = zero_data_median$zero_ch1, 
      zero_ch2 = zero_data_median$zero_ch2,
      .groups = "drop") |> 
    mutate(ch2_sens = cal_slope_ch2) |> 
    pivot_wider(id_cols = date, 
                names_from = type, 
                values_from = c(median_ch2, sampleflow2, zero_ch2, mean_flow, ch2_sens, sampleflow1), 
                names_glue = "{.value}_type{type}")  |> 
    mutate(r2 = mean_flow_typegpt_1_blc_1 / (mean_flow_typegpt_1_blc_1 + (sampleflow2_typegpt_1_blc_1 + sampleflow1_typegpt_1_blc_1)),
           r4 = mean_flow_typegpt_0_blc_1 / (mean_flow_typegpt_0_blc_1 + (sampleflow2_typegpt_0_blc_1 + sampleflow1_typegpt_0_blc_1)), 
           r1 = mean_flow_typegpt_1_blc_0 / (mean_flow_typegpt_1_blc_0 + (sampleflow2_typegpt_1_blc_0 + sampleflow1_typegpt_1_blc_0)), 
           r3 = mean_flow_typegpt_0_blc_0 / (mean_flow_typegpt_0_blc_0 + (sampleflow2_typegpt_0_blc_0 + sampleflow1_typegpt_0_blc_0))) |> 
    mutate(counts_stage1_zero = median_ch2_typegpt_1_blc_0 - zero_ch2_typegpt_1_blc_0,
           counts_stage2_zero = median_ch2_typegpt_1_blc_1 - zero_ch2_typegpt_1_blc_1,
           counts_stage3_zero = median_ch2_typegpt_0_blc_0 - zero_ch2_typegpt_0_blc_0, 
           counts_stage4_zero = median_ch2_typegpt_0_blc_1 - zero_ch2_typegpt_0_blc_1) |> 
    mutate(N = 5.1e3 * (ch2_sens_typegpt_1_blc_0 * 1e3)) |> 
    mutate(part1_eq2 = counts_stage2_zero + (N * r1) - counts_stage1_zero, 
           part2_eq2 = counts_stage4_zero * r2 / r4, 
           part3_eq2 = (N * r1) - counts_stage1_zero) |> 
    mutate(ce_eq2 = (part1_eq2 - part2_eq2) / part3_eq2) 
  
  ce_eq2 <- as.numeric(all_stages$ce_eq2) 
  
  con_eff2 <- bind_rows(gpt_0_blc_1,
                        gpt_1_blc_1,
                        gpt_0_blc_0,
                        gpt_1_blc_0) |>
    mutate(date = floor_date(date, "hour")) |>
    group_by(type, date) |>
    summarise(
      ch1_hz_med = median(ch1_hz, na.rm = TRUE),
      ch2_hz_med = median(ch2_hz, na.rm = TRUE),
      ch1_hz_sd = sd(ch1_hz, na.rm = TRUE),
      ch2_hz_sd = sd(ch2_hz, na.rm = TRUE),
      no_cal_flow_mean = mean(no_cal_flow, na.rm = TRUE),
      flow_sd = sd(no_cal_flow, na.rm = TRUE),
      rxn_vessel_pressure = mean(rxn_vessel_pressure, na.rm = TRUE),
      control_temp = mean(control_temp, na.rm = TRUE), 
      sampleflow1 = median(sampleflow1, na.rm = TRUE), 
      sampleflow2 = median(sampleflow2, na.rm = TRUE), 
      zero_ch1 = zero_data_median$zero_ch1, 
      zero_ch2 = zero_data_median$zero_ch2,
      .groups = "drop") |>
    mutate(ce = con_eff$ce, 
           ce_eq2 = all_stages$ce_eq2) |>
    mutate(ce_flag1 = ifelse(no_cal_flow_mean < 5, 1, 0),
           ce_flag2 = ifelse(ch2_hz_med < 25000, 1, 0))
  
  mean_ch1_sd <- mean(con_eff2$ch1_hz_sd, na.rm = TRUE)
  mean_ch2_sd <- mean(con_eff2$ch2_hz_sd, na.rm = TRUE)
  
  ce_flag1 <- ifelse(any(con_eff2$ce_flag1 == 1, na.rm = TRUE), 1, 0)
  ce_flag2 <- ifelse(any(con_eff2$ce_flag2 == 1, na.rm = TRUE), 1, 0)
  cal_flag2 <- ifelse(any(con_eff2$no_cal_flow_mean < 9.5, na.rm = TRUE), 1, 0)
  cal_flag3 <- ifelse(((mean_ch1_sd + mean_ch2_sd) / 2) > 1000, 1, 0)
  
  all_con_eff <- bind_rows(all_con_eff, con_eff2)
  
  cal_coefficients[i, 1] <- floor_date(cal_timestamp, "hour")
  cal_coefficients[i, 2] <- cal_slope_ch1
  cal_coefficients[i, 3] <- cal_slope_ch2
  cal_coefficients[i, 4] <- ce
  cal_coefficients[i, 5] <- ce_eq2
  cal_coefficients[i, 6] <- no_cal_flow
  cal_coefficients[i, 7] <- mean_ch1_sd
  cal_coefficients[i, 8] <- mean_ch2_sd
  cal_coefficients[i, 9] <- av_control_temp
  cal_coefficients[i, 10] <- av_rxn_vessel_pressure
  cal_coefficients[i, 11] <- cal_flag1
  cal_coefficients[i, 12] <- cal_flag2
  cal_coefficients[i, 13] <- cal_flag3
  cal_coefficients[i, 14] <- ce_flag1
  cal_coefficients[i, 15] <- ce_flag2
}

# Write output files per month/year
cal_coefficients <- cal_coefficients |>
  mutate(date = as.POSIXct(date, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
         month_num = sprintf("%02d", month(date)),
         year = year(date)) |>
  arrange(date) |>
  distinct(.keep_all = TRUE) |> 
  filter(!is.na(date)) 

cal_coefficients |>
  group_by(year, month_num) |>
  group_walk(~ {
    output_dir <- file.path("calibration/viking_tests", .y$year, .y$month_num)
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    write_csv(.x, file.path(output_dir, "coefficients_new_maths.csv"))
  })

all_cal_data <- all_cal_data |>
  mutate(date = as.POSIXct(date),
         month_num = sprintf("%02d", month(date)),
         year = year(date))

all_cal_data |>
  group_by(year, month_num) |>
  group_walk(~ {
    output_dir <- file.path("calibration/viking_tests", .y$year, .y$month_num)
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    write_csv(.x, file.path(output_dir, "sensitivity_params.csv"))
  })

all_con_eff <- all_con_eff |>
  mutate(date = as.POSIXct(date),
         month_num = sprintf("%02d", month(date)),
         year = year(date))

all_con_eff |>
  group_by(year, month_num) |>
  group_walk(~ {
    output_dir <- file.path("calibration/viking_tests", .y$year, .y$month_num)
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    write_csv(.x, file.path(output_dir, "ce_params.csv"))
  })
