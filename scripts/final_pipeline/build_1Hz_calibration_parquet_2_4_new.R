#!/usr/bin/env Rscript
#SBATCH --job-name=cal_1Hz
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --account=chem-cmde-2019
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cw1781@york.ac.uk
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/cal_1Hz_%j.out
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/cal_1Hz_%j.err

# library
library(tidyverse)
library(arrow)
library(waclr)
library(zoo)

# ==== SETTINGS ====
process_from <- "2026-01-01"   # first month to process (NULL = whole record)
process_to   <- NULL           # NULL = up to the last calibration

pair_max_days <- 2.5   # calibration pairs: no more than this many days apart
pair_min_dp   <- 1     # calibration pairs: pressure must change by at least this much

p_ref       <- 10   # reference pressure for normalising (any value works, it cancels out)
outlier_pct <- 10   # remove calibrations more than this % from the median of their 5 neighbours
min_seconds_per_hour <- 600   # an hour needs at least 10 min of pressure data to get a value

# ==== PATHS ====
cal_dir   <- "/mnt/longship/projects/chem-cmde-2019/btt_processing/processing/calibration_data/"
data_root <- "/mnt/longship/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_edit"
out_dir   <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data_v2"

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





# ==== 7a. HOURLY SENSITIVITIES FOR ONE MONTH ====

hourly_sensitivity <- function(df) {
  df %>%
    mutate(hour = floor_date(datetime, "hour")) %>%
    group_by(hour) %>%
    summarise(p_hour = median(av_rxn_vessel_pressure, na.rm = TRUE),   # hourly median pressure
              n_ok   = sum(!is.na(av_rxn_vessel_pressure)),            # seconds of pressure data
              .groups = "drop") %>%
    filter(n_ok >= min_seconds_per_hour) %>%
    mutate(
      mid    = hour + minutes(30),                  # the hourly value belongs to the middle of the hour
      period = get_period(mid),
      # step 2 of the shortcut: interpolate the pressure-free (normalised) calibrations in time
      ch1_norm = interp_within_period(mid, period, cal_norm$date, cal_norm$period, cal_norm$ch1_norm),
      ch2_norm = interp_within_period(mid, period, cal_norm$date, cal_norm$period, cal_norm$ch2_norm),
      # step 3: put this hour's pressure back in
      ch1_sens = ch1_norm * r_ch1^(p_hour - p_ref),
      ch2_sens = ch2_norm * r_ch2^(p_hour - p_ref)
    )
}


test_file <- list.files(data_root, pattern = "2026_01", recursive = TRUE, full.names = TRUE)
test_file   # make sure this is the January 2026 file

df_test <- open_dataset(test_file[1], format = "parquet") %>%
  collect() %>%
  mutate(datetime = parse_excel_date(TheTime, tz = "UTC")) %>%
  rename_all(tolower) %>%
  rename(av_rxn_vessel_pressure = rxn_vessel_pressure) %>%
  arrange(datetime)

hourly_test <- hourly_sensitivity(df_test)
nrow(hourly_test)    # about 744 for a full month

direct_way <- function(h, cal_col, r) {
  cals   <- cal_norm %>% filter(period == h$period)
  before <- cals %>% filter(date <= h$mid) %>% slice_tail(n = 1)
  after  <- cals %>% filter(date >  h$mid) %>% slice_head(n = 1)
  adjust <- function(cal) cal[[cal_col]] * r^(h$p_hour - cal$av_rxn_vessel_pressure)
  if (nrow(before) == 0) return(adjust(after))    # before the first calibration
  if (nrow(after)  == 0) return(adjust(before))   # after the last calibration
  w_after <- as.numeric(difftime(h$mid, before$date, units = "secs")) /
    as.numeric(difftime(after$date, before$date, units = "secs"))
  (1 - w_after) * adjust(before) + w_after * adjust(after)
}

hourly_test <- hourly_test %>%
  mutate(ch1_direct = map_dbl(seq_len(n()), \(i) direct_way(hourly_test[i, ], "ch1_sens", r_ch1)),
         ch2_direct = map_dbl(seq_len(n()), \(i) direct_way(hourly_test[i, ], "ch2_sens", r_ch2)))

max(abs(hourly_test$ch1_direct - hourly_test$ch1_sens))   # should be ~0 (e.g. 1e-15)
max(abs(hourly_test$ch2_direct - hourly_test$ch2_sens))


month_start <- floor_date(min(hourly_test$hour), "month")
month_end   <- month_start + months(1)

cals_month <- cal_sens %>%
  filter(date >= month_start, date < month_end) %>%
  mutate(status = if_else(date %in% cal_outliers$date,
                          "Calibration (removed as outlier)", "Calibration (used)"))

hourly_long <- hourly_test %>%
  select(hour, `Ch1 sensitivity` = ch1_sens, `Ch2 sensitivity` = ch2_sens,
         `Reaction vessel pressure` = p_hour) %>%
  pivot_longer(-hour, names_to = "panel", values_to = "value")

cals_long <- cals_month %>%
  select(date, status, `Ch1 sensitivity` = ch1_sens, `Ch2 sensitivity` = ch2_sens,
         `Reaction vessel pressure` = av_rxn_vessel_pressure) %>%
  pivot_longer(-c(date, status), names_to = "panel", values_to = "value")

ggplot() +
  geom_line(data = hourly_long, aes(hour, value), colour = "grey30", linewidth = 0.4) +
  geom_point(data = cals_long, aes(date, value, colour = status), size = 2.2) +
  facet_wrap(~panel, ncol = 1, scales = "free_y", strip.position = "left") +
  scale_colour_manual(values = c(`Calibration (used)` = "firebrick",
                                 `Calibration (removed as outlier)` = "orange")) +
  labs(x = NULL, y = NULL, colour = NULL,
       title = paste("Hourly sensitivity and pressure,", format(month_start, "%B %Y")),
       subtitle = "Line: hourly values from the new method. Points: 36-hour calibrations.") +
  theme_bw() +
  theme(legend.position = "top", strip.placement = "outside", strip.background = element_blank())

# checking pressure before and after cals 


# ==== CHECK: what does the reaction vessel pressure do around calibrations? ====
# Run after sections 0-2 (needs data_root and cal_sens). Not part of the pipeline.

window_h <- 6                     # hours either side of each calibration to look at
cols_needed <- c("thetime", "rxn_vessel_pressure", "zero_valve_1", "no_valve")

param_files <- list.files(data_root, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)

# the monthly file for e.g. "2026_01" (NA if there isn't one)
month_file <- function(ym) {
  f <- param_files[str_detect(basename(param_files), paste0(ym, "\\.parquet$"))]
  if (length(f) == 0) NA_character_ else f[1]
}

# read only the columns we need from one file (whatever their capitalisation)
read_cols <- function(f, cols) {
  ds   <- open_dataset(f, format = "parquet")
  keep <- names(ds)[tolower(names(ds)) %in% cols]
  ds %>%
    select(all_of(keep)) %>%
    collect() %>%
    rename_with(tolower) %>%
    mutate(datetime = parse_excel_date(thetime, tz = "UTC")) %>%
    select(-thetime)
}

# each month is read once and kept, so repeated calibrations in a month are quick
month_cache <- list()
read_month_cols <- function(ym) {
  if (is.null(month_cache[[ym]])) {
    f <- month_file(ym)
    month_cache[[ym]] <<- if (is.na(f)) tibble() else read_cols(f, cols_needed)
  }
  month_cache[[ym]]
}

# 1-minute median pressure (and valve states) from window_h before to window_h after time t
pressure_around <- function(t) {
  months <- unique(format(c(t - hours(window_h), t, t + hours(window_h)), "%Y_%m"))
  d <- map_dfr(months, read_month_cols)
  if (nrow(d) == 0) return(tibble())
  d %>%
    filter(datetime >= t - hours(window_h), datetime <= t + hours(window_h)) %>%
    mutate(minute = floor_date(datetime, "minute")) %>%
    group_by(minute) %>%
    summarise(pressure = median(rxn_vessel_pressure, na.rm = TRUE),
              zero     = any(zero_valve_1 == 1, na.rm = TRUE),
              no_valve = any(no_valve > 0, na.rm = TRUE),
              .groups = "drop") %>%
    mutate(cal_time = t,
           hours_from_cal = as.numeric(difftime(minute, t, units = "hours")))
}


# ---- A. Examples: two random calibrations per year, plus 7 and 15 Jan 2026 ----
set.seed(1)
example_cals <- cal_sens %>%
  mutate(year = year(date)) %>%
  group_by(year) %>%
  slice_sample(n = 2) %>%
  ungroup() %>%
  bind_rows(filter(cal_sens, date %in% ymd_hms(c("2026-01-07 10:00:00",
                                                 "2026-01-15 03:00:00"), tz = "UTC"))) %>%
  distinct(date, .keep_all = TRUE) %>%
  arrange(date) %>%
  mutate(label = format(date, "%d %b %Y %H:%M"))

examples <- map_dfr(example_cals$date, pressure_around) %>%
  left_join(select(example_cals, cal_time = date, label, p_cal = av_rxn_vessel_pressure),
            by = "cal_time") %>%
  mutate(label = factor(label, levels = example_cals$label),
         state = case_when(no_valve ~ "no_valve > 0",
                           zero     ~ "zero_valve_1 open",
                           TRUE     ~ "neither"))

examples_plot <- ggplot(examples, aes(hours_from_cal, pressure)) +
  geom_line(colour = "grey60", linewidth = 0.3) +
  geom_point(aes(colour = state), size = 0.5) +
  geom_hline(aes(yintercept = p_cal), linetype = "dashed", colour = "firebrick") +
  geom_vline(xintercept = 0, colour = "firebrick") +
  facet_wrap(~label, scales = "free_y") +
  scale_colour_manual(values = c(`no_valve > 0` = "dodgerblue3",
                                 `zero_valve_1 open` = "orange",
                                 neither = "grey30")) +
  labs(x = "Hours from calibration time", y = "Reaction vessel pressure (1-min median)",
       colour = NULL,
       subtitle = "Red line: calibration time. Red dashed: pressure recorded by the calibration.") +
  theme_bw() + theme(legend.position = "top")
examples_plot


# ---- B. Average behaviour: all calibrations in one month per year ----
# Pressure relative to its median 1-6 h before each calibration, then the median
# over all calibrations in that month. A dip at 0 that shows up every year means
# the calibration itself changes the pressure.
epoch_months <- c("2021_03", "2022_06", "2023_06", "2024_06", "2025_07", "2026_01")

epoch <- map_dfr(epoch_months, function(ym) {
  cals <- cal_sens %>% filter(format(date, "%Y_%m") == ym)
  map_dfr(cals$date, pressure_around) %>% mutate(month = ym)
})

epoch_summary <- epoch %>%
  group_by(month, cal_time) %>%
  mutate(anomaly = pressure - median(pressure[hours_from_cal < -1], na.rm = TRUE)) %>%
  group_by(month, hours_from_cal) %>%
  summarise(median_anomaly = median(anomaly, na.rm = TRUE),
            frac_no_valve  = mean(no_valve),
            n_cals         = n_distinct(cal_time),
            .groups = "drop")

epoch_plot <- epoch_summary %>%
  select(month, hours_from_cal,
         `Pressure change from 1-6 h before` = median_anomaly,
         `Fraction of calibrations with no_valve > 0` = frac_no_valve) %>%
  pivot_longer(-c(month, hours_from_cal)) %>%
  ggplot(aes(hours_from_cal, value, colour = month)) +
  geom_vline(xintercept = 0, colour = "grey50") +
  geom_line(linewidth = 0.5) +
  facet_wrap(~name, ncol = 1, scales = "free_y") +
  labs(x = "Hours from calibration time", y = NULL, colour = "Month") +
  theme_bw()
epoch_plot

epoch %>% distinct(month, cal_time) %>% count(month, name = "calibrations")   # how many went into each month





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
