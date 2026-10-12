#!/usr/bin/env Rscript
#SBATCH --job-name=cal_1Hz_v2
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --account=chem-cmde-2019
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cw1781@york.ac.uk
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/cal_1Hz_v2_%j.out
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/cal_1Hz_v2_%j.err

# library
library(tidyverse)
library(arrow)
library(waclr)
library(zoo)

# ==== SETTINGS ====

args     <- commandArgs(trailingOnly = TRUE)                                                              # <<< NEW
run_year <- if (length(args) >= 1) as.integer(args[1]) else as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID", NA))  # <<< NEW

process_from <- NULL   # only used when no year is given (NULL = whole record)                           # <<< CHANGED
process_to   <- NULL   # NULL = up to the last calibration

pair_max_days <- 2.5   # calibration pairs: no more than this many days apart
pair_min_dp   <- 1     # calibration pairs: pressure must change by at least this much

p_ref       <- 10   # reference pressure for normalising (any value works, it cancels out)
min_seconds_per_hour <- 600   # an hour needs at least 10 min of pressure data to get a value

outlier_pct <- 30   # only remove a calibration if it is more than this % from the median of its 5 neighbours

# ==== PATHS ====
cal_dir   <- "/mnt/longship/projects/chem-cmde-2019/btt_processing/processing/calibration_data/"
data_root <- "/mnt/longship/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_edit"
out_dir   <- "/mnt/longship/projects/chem-bttao-2026/btt_processing/processing/1Hz_cal_data_v2"

period_starts <- ymd_hms(c(
  "2020-10-13 02:00:00",  # 2
  "2021-01-11",           # 3
  "2021-06-11",           # 4
  "2021-12-31",           # 5
  "2022-11-01",           # 6  pump maintenance
  "2023-10-19",           # 7  unexplained pressure change
  "2024-02-08",           # 8  pump replaced
  "2025-02-03",           # 9  instrument back after maintenance (cylinder and pump change)
  "2025-03-18",           # 10 pump moved in front of rack
  "2025-06-05",           # 11
  "2025-11-13",           # 12 pump replaced
  "2025-12-30",           # 13
  "2026-07-16",           # 14 Tip seal change 
  "2026-08-12 08:00:00"   # 15 cylinder change
), tz = "UTC", truncated = 3)  

get_period <- function(datetime) {
  paste0("period", findInterval(datetime, period_starts) + 1)
}


# ==== 2. CALIBRATIONS ====

# read all calibration files once
cal_raw <- list.files(cal_dir, full.names = TRUE,
                      pattern = "coefficients", recursive = TRUE) %>%
  map_df(read_csv, show_col_types = FALSE) %>%
  mutate(date = as_datetime(date, tz = "UTC")) %>%
  filter(!no_cal_flow < 5) %>%   # no-cal flow went below 5 (set at 10 sccm)
  filter(!date %in% as_datetime(c("2025-10-17 12:00:00",   # dodgy cal after restart
                                  "2021-01-14 09:00:00",   # inlet pressure messed up
                                  "2021-01-12 21:00:00"),  # inlet pressure messed up
                                tz = "UTC")) %>%
  arrange(date) %>%
  distinct(date, .keep_all = TRUE)        # drop any calibration read in twice

# for the CE interpolation (section 6): same filters as the original
cal_coefficients <- cal_raw %>%
  filter(ch1_sens >= 1, ch2_sens >= 1)

# for sensitivity: genuine sensitivities can be < 1 at high pressure, so only
# remove values that are clearly broken; the outlier check (section 4) does the rest
cal_sens <- cal_raw %>%
  filter(ch1_sens > 0.1, ch2_sens > 0.1,  # clearly broken (close to zero)
         ce > 0, ce < 1,                  # bad CE
         av_rxn_vessel_pressure < 400,    # silly high pressure
         ch1_sens < 10) %>%               # outlier
  mutate(period = get_period(date))

message(nrow(cal_raw), " calibrations read; ", nrow(cal_sens), " used for sensitivity")

# if no end date is set, process up to the last calibration
if (is.null(process_to)) process_to <- as.character(as_date(max(cal_raw$date)))


# ==== 3. PRESSURE EFFECT (r) FROM CALIBRATION PAIRS ====

cal_pairs <- cal_sens %>%
  select(date, period, pressure = av_rxn_vessel_pressure, ch1 = ch1_sens, ch2 = ch2_sens) %>%
  pivot_longer(c(ch1, ch2), names_to = "channel", values_to = "sens") %>%
  group_by(channel, period) %>%                       # never pair across a period break
  arrange(date, .by_group = TRUE) %>%
  mutate(
    days  = as.numeric(difftime(date, lag(date), units = "days")),  # time since previous cal
    dp    = pressure - lag(pressure),                               # pressure change
    ratio = sens / lag(sens)                                        # sensitivity after / before
  ) %>%
  ungroup() %>%
  filter(days <= pair_max_days, abs(dp) >= pair_min_dp) %>%         # close in time, big pressure change
  mutate(r_pair = ratio^(1 / dp))                                   # multiplier per unit pressure

r_ch1 <- median(cal_pairs$r_pair[cal_pairs$channel == "ch1"])
r_ch2 <- median(cal_pairs$r_pair[cal_pairs$channel == "ch2"])

message(sprintf("r from %d pairs: Ch1 r = %.3f (%.1f%% per unit), Ch2 r = %.3f (%.1f%% per unit)",
                sum(cal_pairs$channel == "ch1"),
                r_ch1, 100 * (r_ch1 - 1), r_ch2, 100 * (r_ch2 - 1)))



# ==== 4. NORMALISE CALIBRATIONS TO p_ref AND REMOVE OUTLIERS ====

cal_norm <- cal_sens %>%
  group_by(period) %>%                       # neighbours are only compared within a period
  mutate(
    # what each calibration would have read at pressure p_ref
    ch1_norm = ch1_sens * r_ch1^(p_ref - av_rxn_vessel_pressure),
    ch2_norm = ch2_sens * r_ch2^(p_ref - av_rxn_vessel_pressure),
    # % difference from the median of the 5 calibrations around it (itself included)
    ch1_dev = 100 * (ch1_norm / rollapply(ch1_norm, 5, median, partial = TRUE) - 1),
    ch2_dev = 100 * (ch2_norm / rollapply(ch2_norm, 5, median, partial = TRUE) - 1),
    outlier = abs(ch1_dev) > outlier_pct | abs(ch2_dev) > outlier_pct
  ) %>%
  ungroup()

cal_outliers <- cal_norm %>% filter(outlier)
cal_norm     <- cal_norm %>% filter(!outlier)

message(nrow(cal_outliers), " calibrations removed as outliers (> ", outlier_pct,
        "% from their neighbours); ", nrow(cal_norm), " used for sensitivity")

# ==== 5. INTERPOLATION WITHIN A PERIOD ====

# Linear interpolation in time that never crosses a period break.
#   x, x_period:                       the times we want values for, and their periods
#   known_x, known_period, known_y:    the times, periods and values we interpolate from
interp_within_period <- function(x, x_period, known_x, known_period, known_y) {
  out <- rep(NA_real_, length(x))
  for (p in unique(na.omit(x_period))) {
    i <- which(x_period == p)                          # times in this period
    j <- which(known_period == p & !is.na(known_y))    # known values in this period
    if (length(j) == 0) next                           # no calibrations: leave as NA
    if (length(j) == 1) { out[i] <- known_y[j]; next } # one calibration: use it everywhere
    out[i] <- approx(as.numeric(known_x[j]), known_y[j],
                     xout = as.numeric(x[i]), rule = 2)$y
  }
  out
}

# ==== 6. CONVERSION EFFICIENCY ====

# define interpolation windows - based on previous assessment of CE data 
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

ce_interp <- interpolate_ce %>%
  select(date, ce_interpolated) %>%
  rename("ce" = ce_interpolated) %>% 
  arrange(date)

# ==== 7. 1 Hz PROCESSING (same as the original, except how ch1_sens and ch2_sens are made) ====

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# record what went into this run (used by the plot script too)
# write to a temporary name, then rename                                   
write_csv_safely <- function(x, path) {                                                               
  tmp <- paste0(path, ".tmp_", Sys.getpid())                                                          
  write_csv(x, tmp)                                                                                   
  file.rename(tmp, path)                                                                              
}                                                                                                     
write_csv_safely(cal_norm %>% select(date, period, av_rxn_vessel_pressure, ch1_sens, ch2_sens,      
                                     ch1_norm, ch2_norm, ch1_dev, ch2_dev),
                 file.path(out_dir, "calibrations_used.csv"))
write_csv_safely(cal_outliers %>% select(date, period, av_rxn_vessel_pressure, ch1_sens, ch2_sens,   
                                         ch1_dev, ch2_dev),
                 file.path(out_dir, "calibrations_outliers_removed.csv"))
write_csv_safely(tibble(run_date = Sys.time(), channel = c("ch1", "ch2"), r = c(r_ch1, r_ch2),       
                        pct_per_unit = 100 * (c(r_ch1, r_ch2) - 1),
                        n_pairs = c(sum(cal_pairs$channel == "ch1"), sum(cal_pairs$channel == "ch2")),
                        p_ref = p_ref),
                 file.path(out_dir, "pressure_effect_r.csv"))

# does a parquet file contain data between `from` and `to`? 
file_in_range <- function(f, from, to) {
  rng <- open_dataset(f, format = "parquet") |>
    select(TheTime) |>
    summarise(mn = min(TheTime, na.rm = TRUE), mx = max(TheTime, na.rm = TRUE)) |>
    collect()
  if (nrow(rng) == 0 || is.na(rng$mx)) return(FALSE)
  mn <- parse_excel_date(rng$mn, tz = "UTC")
  mx <- parse_excel_date(rng$mx, tz = "UTC")
  after_start <- is.null(from) || mx >= as.POSIXct(from, tz = "UTC")
  before_end  <- is.null(to)   || mn <= as.POSIXct(to, tz = "UTC") + days(1)
  after_start && before_end
}

param_files <- list.files(data_root, full.names = TRUE, pattern = "\\.parquet$", recursive = TRUE)

if (!is.na(run_year)) {                                                                               
  # one year: pick that year's monthly files by name (param_YYYY_MM.parquet)                         
  param_files <- param_files[str_detect(basename(param_files), paste0("param_", run_year, "_"))]     
  message("Year ", run_year, ": processing ", length(param_files), " files")                         
} else {                                                                                             
  param_files <- param_files[map_lgl(param_files, file_in_range, from = process_from, to = process_to)]
  message("Processing ", length(param_files), " files (", process_from, " to ", process_to, ")")    
}                                                                                                    
n_files <- length(param_files)
message(paste(basename(param_files), collapse = ", "))                                              

for (i in seq_along(param_files)) {
  f <- param_files[i]
  message(sprintf("[%d/%d] Processing: %s", i, n_files, basename(f)))
  
  # --- read one month
  df <- open_dataset(f, format = "parquet") %>%
    collect() %>%
    mutate(datetime = parse_excel_date(TheTime, tz = "UTC")) %>%
    rename_all(tolower) %>%
    rename(av_rxn_vessel_pressure = rxn_vessel_pressure) %>%
    arrange(datetime)
  
  if (nrow(df) == 0) {
    message("Skipping empty file: ", basename(f))
    next
  }
  
  # --- NEW: hourly sensitivities
  #   1. median pressure for each hour
  #   2. normalised calibrations interpolated in time to the middle of the hour
  #   3. converted to the hour's pressure with r^(p_hour - p_ref)
  hourly <- df %>%
    mutate(hour = floor_date(datetime, "hour")) %>%
    group_by(hour) %>%
    summarise(p_hour = median(av_rxn_vessel_pressure, na.rm = TRUE),
              n_ok   = sum(!is.na(av_rxn_vessel_pressure)),
              .groups = "drop") %>%
    filter(n_ok >= min_seconds_per_hour,        # at least 10 min of pressure data
           p_hour >= 1, p_hour <= 40) %>%       # instrument on and in range
    mutate(mid      = hour + minutes(30),
           period   = get_period(mid),
           ch1_norm = interp_within_period(mid, period, cal_norm$date, cal_norm$period, cal_norm$ch1_norm),
           ch2_norm = interp_within_period(mid, period, cal_norm$date, cal_norm$period, cal_norm$ch2_norm),
           ch1_sens = ch1_norm * r_ch1^(p_hour - p_ref),
           ch2_sens = ch2_norm * r_ch2^(p_hour - p_ref))
  
  # --- CE, period and sensitivities
  df <- df %>%
    mutate(
      ce     = approx(ce_interp$date, ce_interp$ce, xout = datetime, rule = 2)$y,
      period = get_period(datetime),
      # NEW: linear interpolation between the hourly sensitivities (never across a period break)
      ch1_sens = interp_within_period(datetime, period, hourly$mid, hourly$period, hourly$ch1_sens),
      ch2_sens = interp_within_period(datetime, period, hourly$mid, hourly$period, hourly$ch2_sens)
    ) %>%
    # --- final physical filters
    mutate(
      bad = av_rxn_vessel_pressure > 40 | inlet_pressure < 199 | inlet_pressure > 350,
      ch1_sens = ifelse(bad, NA_real_, ch1_sens),
      ch2_sens = ifelse(bad, NA_real_, ch2_sens),
      ce       = ifelse(bad, NA_real_, ce)
    ) %>%
    select(-bad)
  
  # --- zeros
  zero_data <- df %>%
    filter(zero_valve_1 == 1.0, zero_valve_2 == 1.0) %>%
    mutate(second = second(datetime),
           hour_block = floor_date(datetime, "hour")) %>%
    filter(between(second, 5, 15)) %>%
    group_by(hour_block) %>%   # only the zeros at the start of each hour
    summarise(date   = min(datetime),
              ch1_hz = median(ch1_hz, na.rm = TRUE),
              ch2_hz = median(ch2_hz, na.rm = TRUE),
              .groups = "drop") %>%
    arrange(date)
  
  if (nrow(zero_data) > 0) {
    df <- df %>%
      mutate(ch1_zero = approx(zero_data$date, zero_data$ch1_hz, xout = datetime, rule = 2)$y,
             ch2_zero = approx(zero_data$date, zero_data$ch2_hz, xout = datetime, rule = 2)$y)
  } else {
    df <- df %>% mutate(ch1_zero = NA_real_, ch2_zero = NA_real_)
  }
  
  # --- write, one subfolder per year, without R metadata
  year_dir <- file.path(out_dir, as.character(year(min(df$datetime, na.rm = TRUE))))
  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
  out_file <- file.path(year_dir, basename(f))
  
  tbl <- arrow_table(df)
  tbl$metadata$r <- NULL
  write_parquet(tbl, out_file)
  
  # NEW: hourly values next to the parquet file (for the plot script)
  write_csv(hourly, sub("\\.parquet$", "_hourly_sens.csv", out_file))
  
  message("Saved: ", out_file)
}