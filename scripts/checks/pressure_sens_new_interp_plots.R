# ==== Diagnostics 1: calibrations, pressure effect and CE (nothing is processed or saved) ====
library(tidyverse)

script   <- "cal_1Hz_v2.R"   # your processing script
diag_dir <- "diagnostics"    # where the plots go
dir.create(diag_dir, showWarnings = FALSE)

# run sections 0-6 of the processing script only (stops before section 7, so no files are written)
lines <- readLines(script)
eval(parse(text = lines[1:(grep("^# ==== 7\\.", lines) - 1)]))


# ---- D1. Is r sensible? Each calibration pair against the fitted r ----
# Each point is one pair (< pair_max_days apart, pressure changed by >= pair_min_dp).
# If the pressure effect is right, the points scatter around the line r^dp.
r_line <- tibble(dp = seq(min(cal_pairs$dp), max(cal_pairs$dp), length.out = 200)) %>%
  crossing(channel = c("ch1", "ch2")) %>%
  mutate(ratio = if_else(channel == "ch1", r_ch1, r_ch2)^dp)

d1 <- ggplot(cal_pairs, aes(dp, ratio)) +
  geom_hline(yintercept = 1, colour = "grey70") +
  geom_point(alpha = 0.6) +
  geom_line(data = r_line, colour = "#D55E00", linewidth = 0.8) +
  facet_wrap(~channel, labeller = as_labeller(c(
    ch1 = sprintf("Ch1: r = %.3f (%.1f%% per unit)", r_ch1, 100 * (r_ch1 - 1)),
    ch2 = sprintf("Ch2: r = %.3f (%.1f%% per unit)", r_ch2, 100 * (r_ch2 - 1))))) +
  scale_y_log10() +
  labs(x = "Pressure change between the two calibrations", y = "Sensitivity after / before (log scale)",
       title = paste("D1. Pressure effect from", sum(cal_pairs$channel == "ch1"), "calibration pairs"),
       subtitle = "Orange line: r^(pressure change). Points should scatter evenly around it.") +
  theme_bw()
print(d1)

# ---- D1b. The calculation for each pair, one channel ----
# One row per pair (time order): 1. pressure change, 2. sensitivity change (%), 3. the r from those two.
viz_channel <- "ch1"    # "ch1" or "ch2"

pair_calc <- cal_pairs %>% filter(channel == viz_channel)
r_med     <- median(pair_calc$r_pair)

pair_long <- pair_calc %>%
  mutate(pair_label = format(date, "%d %b %Y %H:%M"),                            # date of the second calibration
         pair_label = fct_reorder(pair_label, as.numeric(date), .desc = TRUE),  # earliest pair at the top
         year       = factor(year(date))) %>%
  transmute(pair_label, year,
            `1. Pressure change`                  = dp,
            `2. Sensitivity change (%)`           = 100 * (ratio - 1),
            `3. r = ratio^(1 / pressure change)`  = r_pair) %>%
  pivot_longer(-c(pair_label, year), names_to = "panel", values_to = "value")

ref_lines <- tibble(panel = c("1. Pressure change", "2. Sensitivity change (%)", "3. r = ratio^(1 / pressure change)"),
                    value = c(0, 0, r_med))

d1b <- ggplot(pair_long, aes(value, pair_label)) +
  geom_vline(data = ref_lines, aes(xintercept = value), linetype = "dashed", colour = "grey40") +
  geom_col(data = filter(pair_long, !str_starts(panel, "3")), aes(fill = year), width = 0.7) +
  geom_point(data = filter(pair_long, str_starts(panel, "3")), aes(colour = year), size = 2.5) +
  facet_wrap(~panel, nrow = 1, scales = "free_x") +
  labs(x = NULL, y = "Calibration pair (date of second calibration)", fill = "Year", colour = "Year",
       title = paste0(toupper(viz_channel), ": the calculation for each pair"),
       subtitle = sprintf("Pairs in time order. Dashed line in panel 3 = median r = %.3f (%.1f%% per unit pressure)",
                          r_med, 100 * (r_med - 1))) +
  theme_bw() +
  theme(axis.text.y = element_text(size = 6))
print(d1b)


# ---- D1c. Every pair against the fitted pressure effect ----
r_by_channel <- cal_pairs %>%
  group_by(channel) %>%
  summarise(n_pairs = n(), r = median(r_pair), .groups = "drop") %>%
  mutate(label = sprintf("%.1f%% per unit pressure\n(%d pairs)", 100 * (r - 1), n_pairs))

rule_curve <- r_by_channel %>%
  crossing(dp = seq(floor(min(cal_pairs$dp)), ceiling(max(cal_pairs$dp)), by = 0.1)) %>%
  mutate(pct_change = 100 * (r^dp - 1))

channel_names <- as_labeller(c(ch1 = "Ch1", ch2 = "Ch2"))

d1c <- cal_pairs %>%
  ggplot(aes(dp, 100 * (ratio - 1))) +
  geom_hline(yintercept = 0, colour = "grey70") +
  geom_vline(xintercept = 0, colour = "grey70") +
  geom_line(data = rule_curve, aes(dp, pct_change), linewidth = 1) +
  geom_point(aes(colour = factor(year(date))), size = 2.5, alpha = 0.8) +
  geom_text(data = r_by_channel, aes(x = Inf, y = Inf, label = label), hjust = 1.1, vjust = 1.3, size = 4) +
  facet_wrap(~channel, labeller = channel_names) +
  labs(x = paste0("Pressure change between calibrations < ", pair_max_days, " days apart"),
       y = "Change in sensitivity (%)", colour = "Year",
       caption = "Black line: fitted pressure effect, 100 x (r^pressure change - 1)") +
   #coord_cartesian(xlim = c(-10, 10), ylim = c(-100, 300)) +
  theme_bw(base_size = 13)
print(d1c)


# ---- D1d. Measured sensitivity against the one predicted from the previous calibration ----
# predicted = previous calibration's sensitivity x r^(pressure change); points on the dashed line = rule works
d1d <- cal_pairs %>%
  left_join(select(r_by_channel, channel, r_all = r), by = "channel") %>%
  mutate(sens_before = sens / ratio,
         predicted   = sens_before * r_all^dp) %>%
  ggplot(aes(predicted, sens)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  geom_point(aes(colour = factor(year(date))), size = 2) +
  facet_wrap(~channel, scales = "free", labeller = channel_names) +
  labs(x = "Sensitivity predicted from previous calibration + pressure change",
       y = "Measured sensitivity", colour = "Year") +
  theme_bw()
print(d1d)


# ---- D2. Calibrations converted to p_ref, whole record ----
# If r is right, converting to the same pressure should leave a fairly smooth line within each
# period, with steps only at period breaks (dashed lines). Removed calibrations in orange.
cal_all <- bind_rows(cal_norm %>% mutate(type = "Used"),
                     cal_outliers %>% mutate(type = "Removed (> outlier_pct)"))
panels <- c(paste("Ch1 at p =", p_ref), paste("Ch2 at p =", p_ref), "Calibration pressure")

d2 <- cal_all %>%
  select(date, type, ch1_norm, ch2_norm, av_rxn_vessel_pressure) %>%
  pivot_longer(-c(date, type), names_to = "var", values_to = "value") %>%
  mutate(panel = factor(recode(var, ch1_norm = panels[1], ch2_norm = panels[2],
                               av_rxn_vessel_pressure = panels[3]), panels)) %>%
  arrange(desc(type)) %>%   # draw used points first so removed ones sit on top
  ggplot(aes(date, value, colour = type, shape = type)) +
  geom_vline(xintercept = period_starts, linetype = "dashed", colour = "grey60", linewidth = 0.3) +
  geom_point(size = 1) +
  facet_wrap(~panel, ncol = 1, scales = "free_y", strip.position = "left") +
  scale_colour_manual(values = c("Used" = "black", "Removed (> outlier_pct)" = "#D55E00")) +
  scale_shape_manual(values = c("Used" = 16, "Removed (> outlier_pct)" = 17)) +
  scale_x_datetime(date_breaks = "6 months", date_labels = "%b %Y") +
  labs(x = NULL, y = NULL, colour = NULL, shape = NULL,
       title = "D2. Calibrations converted to the same pressure",
       subtitle = "Dashed lines: period breaks. Within a period the top two panels should not jump around with the pressure panel.") +
  theme_bw() +
  theme(legend.position = "top", strip.placement = "outside", strip.background = element_blank())
print(d2)
ggsave(file.path(diag_dir, "D2_calibrations_normalised.png"), d2, width = 12, height = 8, dpi = 200)


# ---- D3. Does anything still depend on pressure after normalising? ----
# Within each period, compare each calibration with the one before it: if r is right, the change in
# normalised sensitivity should not depend on the pressure change (flat orange line).
d3_data <- cal_norm %>%
  group_by(period) %>%
  arrange(date, .by_group = TRUE) %>%
  mutate(days = as.numeric(difftime(date, lag(date), units = "days")),
         dp   = av_rxn_vessel_pressure - lag(av_rxn_vessel_pressure),
         Ch1  = 100 * (ch1_norm / lag(ch1_norm) - 1),
         Ch2  = 100 * (ch2_norm / lag(ch2_norm) - 1)) %>%
  ungroup() %>%
  filter(days <= pair_max_days) %>%
  pivot_longer(c(Ch1, Ch2), names_to = "channel", values_to = "pct_change")

d3 <- ggplot(d3_data, aes(dp, pct_change)) +
  geom_hline(yintercept = 0, colour = "grey60") +
  geom_point(alpha = 0.4, size = 1) +
  geom_smooth(method = "lm", formula = y ~ x, colour = "#D55E00", se = TRUE) +
  facet_wrap(~channel) +
  coord_cartesian(ylim = c(-30, 30)) +
  labs(x = "Pressure change since the previous calibration",
       y = "% change in normalised sensitivity",
       title = "D3. After normalising, is there any pressure effect left?",
       subtitle = "Consecutive calibrations < pair_max_days apart. A flat orange line means r has removed the pressure effect.") +
  theme_bw()
print(d3)
ggsave(file.path(diag_dir, "D3_leftover_pressure_effect.png"), d3, width = 10, height = 5, dpi = 200)


# ---- D4. CE: calibration values and what will be used ----
d4 <- ggplot() +
  geom_point(data = cal_coefficients %>% filter(ce_zero > 0, ce_zero < 1),
             aes(date, ce_zero, colour = "Calibration ce_zero"), size = 0.8) +
  geom_line(data = ce_interp, aes(date, ce, colour = "ce used for 1 Hz"), linewidth = 0.5) +
  scale_colour_manual(values = c("Calibration ce_zero" = "grey60", "ce used for 1 Hz" = "#0072B2")) +
  scale_x_datetime(date_breaks = "6 months", date_labels = "%b %Y") +
  labs(x = NULL, y = "CE", colour = NULL,
       title = "D4. Conversion efficiency",
       subtitle = "Blue: ce_interp (interpolated within the interp_ranges windows, 0.45-0.7 kept).") +
  theme_bw() + theme(legend.position = "top")
print(d4)
ggsave(file.path(diag_dir, "D4_ce.png"), d4, width = 12, height = 5, dpi = 200)


# ---- numbers to check ----
message(sprintf("Calibrations: %d read, %d pass the filters, %d removed as outliers, %d used",
                nrow(cal_raw), nrow(cal_sens), nrow(cal_outliers), nrow(cal_norm)))
message(sprintf("r: Ch1 %.3f, Ch2 %.3f from %d pairs", r_ch1, r_ch2, sum(cal_pairs$channel == "ch1")))
message("Calibrations used per period:")
print(count(cal_norm, period), n = Inf)
message("Removed as outliers:")
print(cal_outliers %>% select(date, period, av_rxn_vessel_pressure, ch1_sens, ch2_sens, ch1_dev, ch2_dev), n = Inf)
message(sprintf("CE: %d values from %s to %s, range %.2f-%.2f",
                nrow(ce_interp), min(ce_interp$date), max(ce_interp$date),
                min(ce_interp$ce), max(ce_interp$ce)))


all_files <- list.files(data_root, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)

hourly_pressure_file <- function(f) {
  ds   <- open_dataset(f, format = "parquet")
  keep <- names(ds)[tolower(names(ds)) %in% c("thetime", "rxn_vessel_pressure", "no_valve")]
  d <- ds %>% select(all_of(keep)) %>% collect() %>% rename_with(tolower)
  if (!"no_valve" %in% names(d)) d$no_valve <- NA_real_    # in case a file doesn't have it
  d %>%
    mutate(hour = floor_date(parse_excel_date(thetime, tz = "UTC"), "hour")) %>%
    group_by(hour) %>%
    summarise(p_hour   = median(rxn_vessel_pressure, na.rm = TRUE),   # hourly median pressure
              p_sd     = sd(rxn_vessel_pressure, na.rm = TRUE),       # variability within the hour
              n_ok     = sum(!is.na(rxn_vessel_pressure)),            # seconds of pressure data
              frac_cal = mean(no_valve > 0, na.rm = TRUE),            # fraction of the hour with cal gas on
              .groups = "drop")
}

hourly_all <- map(all_files, hourly_pressure_file, .progress = TRUE) %>%
  list_rbind() %>%
  filter(n_ok >= min_seconds_per_hour) %>%
  arrange(hour) %>%
  distinct(hour, .keep_all = TRUE) %>%                        # in case two files overlap
  mutate(year     = year(hour),
         cal_hour = coalesce(frac_cal > 0.1, FALSE),          # cal gas on for more than 6 min
         # start a new line segment after gaps of more than 2 hours, so gaps aren't joined up
         segment  = cumsum(coalesce(as.numeric(difftime(hour, lag(hour), units = "hours")) > 2, TRUE)))

write_csv(hourly_all, "data/data/hourly_pressure_all.csv")   

hourly_all = read_csv("data/data/hourly_pressure_all.csv")

# ---- plot: one panel per year ----
pressure_by_year <- hourly_all %>%
  filter(p_hour < 60) %>%                                     # drop instrument-off values (e.g. 500)
  ggplot(aes(hour, p_hour)) +
  geom_line(aes(group = segment), colour = "grey40", linewidth = 0.25) +
  geom_point(data = ~ filter(.x, cal_hour), colour = "firebrick", size = 0.7) +
  facet_wrap(~year, ncol = 1, scales = "free") +
  scale_x_datetime(date_labels = "%b", date_breaks = "1 month") +
  labs(x = NULL, y = "Hourly median reaction vessel pressure",
       subtitle = "Red: calibration hours") +
  theme_bw()
pressure_by_year

ggsave("hourly_pressure_by_year.png", pressure_by_year, width = 14, height = 18, dpi = 150)


# ---- D5. Hourly sensitivities and hourly pressure, one plot per year ----
# Builds the hourly values the processing script will make, without running it: reads only the
# time and pressure columns from each 1 Hz file and applies the same hourly step as section 7.
# The result is saved, so later re-plots don't need to read the 1 Hz files again.
# ---- D5. Hourly sensitivities and hourly pressure, one plot per year ----
# Builds the hourly values the processing script will make, without running it: reads only the
# time and pressure columns from each 1 Hz file and applies the same hourly step as section 7.
files <- list.files(data_root, full.names = TRUE, pattern = "\\.parquet$", recursive = TRUE)

hourly_all <- map_df(files, function(f) {
  message("Reading pressure: ", basename(f))
  ds   <- open_dataset(f, format = "parquet")
  pcol <- names(ds)[tolower(names(ds)) == "rxn_vessel_pressure"]   # column name as stored in the file
  ds %>%
    select(TheTime, p = all_of(pcol)) %>%
    collect() %>%
    mutate(hour = floor_date(parse_excel_date(TheTime, tz = "UTC"), "hour")) %>%
    group_by(hour) %>%
    summarise(p_hour = median(p, na.rm = TRUE),
              n_ok   = sum(!is.na(p)),
              .groups = "drop")
}) %>%
  # same as the hourly step in section 7
  filter(n_ok >= min_seconds_per_hour, p_hour >= 1, p_hour <= 40) %>%
  mutate(mid      = hour + minutes(30),
         period   = get_period(mid),
         ch1_norm = interp_within_period(mid, period, cal_norm$date, cal_norm$period, cal_norm$ch1_norm),
         ch2_norm = interp_within_period(mid, period, cal_norm$date, cal_norm$period, cal_norm$ch2_norm),
         ch1_sens = ch1_norm * r_ch1^(p_hour - p_ref),
         ch2_sens = ch2_norm * r_ch2^(p_hour - p_ref)) %>%
  arrange(hour)

panels <- c("Ch1 sensitivity", "Ch2 sensitivity", "Hourly pressure")

# hourly lines; a new segment starts after any missing hour so lines don't bridge gaps
d5_lines <- hourly_all %>%
  mutate(segment = cumsum(c(1, diff(as.numeric(hour)) > 3600))) %>%
  select(hour, segment, ch1_sens, ch2_sens, p_hour) %>%
  pivot_longer(c(ch1_sens, ch2_sens, p_hour), names_to = "var", values_to = "value") %>%
  mutate(panel = factor(recode(var, ch1_sens = panels[1], ch2_sens = panels[2], p_hour = panels[3]), panels))

# calibrations: measured sensitivities, and the pressure during each calibration
d5_points <- bind_rows(cal_norm %>% mutate(type = "Calibration"),
                       cal_outliers %>% mutate(type = "Calibration removed")) %>%
  select(date, type, ch1_sens, ch2_sens, av_rxn_vessel_pressure) %>%
  pivot_longer(c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), names_to = "var", values_to = "value") %>%
  mutate(panel = factor(recode(var, ch1_sens = panels[1], ch2_sens = panels[2],
                               av_rxn_vessel_pressure = panels[3]), panels))

for (yr in sort(unique(year(hourly_all$hour)))) {
  l <- d5_lines  %>% filter(year(hour) == yr)
  p <- d5_points %>% filter(year(date) == yr)
  
  g <- ggplot() +
    geom_vline(xintercept = period_starts[year(period_starts) == yr],
               linetype = "dashed", colour = "grey60", linewidth = 0.3) +
    geom_line(data = l, aes(hour, value, group = segment), linewidth = 0.3) +
    geom_point(data = p, aes(date, value, colour = type, shape = type), size = 1.3) +
    facet_wrap(~panel, ncol = 1, scales = "free_y", strip.position = "left") +
    scale_colour_manual(values = c("Calibration" = "red", "Calibration removed" = "orange")) +
    scale_shape_manual(values = c("Calibration" = 16, "Calibration removed" = 4)) +
    scale_x_datetime(date_breaks = "1 month", date_labels = "%b",
                     limits = as_datetime(c(paste0(yr, "-01-01"), paste0(yr + 1, "-01-01")), tz = "UTC")) +
    labs(x = NULL, y = NULL, colour = NULL, shape = NULL,
         title = paste("Hourly sensitivities and hourly pressure,", yr),
         subtitle = "Black lines: hourly values. Red dots: calibrations. Dashed lines: period breaks.") +
    theme_bw() +
    theme(legend.position = "top", strip.placement = "outside", strip.background = element_blank())
  
  print(g)
}



# ---- D6. Does the new method predict calibrations better than the original? ----
# Each calibration is left out in turn and predicted from the others with
#   - new:       normalised calibrations interpolated in time within the period, x r^(p - p_ref)
#   - original:  per-period straight-line pressure model (lm_periods, within the calibrated
#                pressure range), otherwise time interpolation over all calibrations
#   - time only: plain time interpolation within the period, no pressure correction (baseline)
# and compared with what it actually measured, using the calibration's own pressure.
# Uses only the calibrations loaded at the top (no 1 Hz files), takes about a minute.

# original method's periods (copied from the original script)
orig_fit_period <- function(d) case_when(          # periods used to FIT the straight lines
  d <= ymd_hms("2020-10-13 01:00:00") ~ "period1",
  d <  ymd("2021-01-11") ~ "period2",  d < ymd("2021-06-11") ~ "period3",
  d <  ymd("2021-12-31") ~ "period4",  d < ymd("2022-11-01") ~ "period5",
  d <  ymd("2023-10-19") ~ "period6",  d < ymd("2024-02-08") ~ "period7",
  d <  ymd("2024-08-31") ~ "period8",  d < ymd("2025-03-18") ~ "period9",
  d <  ymd("2025-06-05") ~ "period10", d < ymd("2025-11-13") ~ "period11",
  d <  ymd("2025-12-30") ~ "period12", d < ymd("2026-07-16") ~ "period13",
  TRUE ~ "period14")
orig_apply_period <- function(d) case_when(        # periods used to APPLY them (get_period)
  d <= ymd_hms("2020-10-13 01:00:00") ~ "period1",
  d <  ymd("2021-01-11") ~ "period2",  d < ymd("2021-06-11") ~ "period3",
  d <  ymd("2021-12-31") ~ "period4",  d < ymd("2022-11-01") ~ "period5",
  d <  ymd("2023-10-19") ~ "period6",  d < ymd("2024-02-08") ~ "period7",
  d <  ymd("2025-02-03") ~ "period8",  d < ymd("2025-03-18") ~ "period9",
  d <  ymd("2025-06-05") ~ "period10", d < ymd("2025-11-13") ~ "period11",
  d <  ymd("2025-12-30") ~ "period12", d < ymd("2026-07-16") ~ "period13",
  TRUE ~ "period14")
orig_lm_periods <- c("period1", "period3", "period4", "period7", "period8",
                     "period10", "period11", "period12", "period13")

cal_orig <- cal_raw %>%                              # original calibration filters
  filter(ch1_sens >= 1, ch2_sens >= 1) %>%
  mutate(fit_period = orig_fit_period(date))

predict_original <- function(t, p, ch, train) {
  sens  <- train[[paste0(ch, "_sens")]]
  t_int <- approx(as.numeric(train$date), sens, xout = as.numeric(t), rule = 2)$y
  per   <- orig_apply_period(t)
  if (!per %in% orig_lm_periods || p > 40) return(t_int)
  d <- train %>% filter(fit_period == per, av_rxn_vessel_pressure < 400, ch1_sens < 10)
  if (nrow(d) < 3 || p < min(d$av_rxn_vessel_pressure) || p > max(d$av_rxn_vessel_pressure)) return(t_int)
  b <- coef(lm(d[[paste0(ch, "_sens")]] ~ d$av_rxn_vessel_pressure))
  b[[1]] + b[[2]] * p
}

# leave each calibration out in turn
targets <- cal_norm
loo <- map_dfr(seq_len(nrow(targets)), function(k) {
  tk <- targets[k, ]; t <- tk$date; pk <- tk$av_rxn_vessel_pressure; per <- tk$period
  wo  <- cal_norm[-k, ]
  tr0 <- cal_orig %>% filter(date != t)
  # work everything out first, then build the table (tibble() would recycle pk inside itself)
  new_1  <- interp_within_period(t, per, wo$date, wo$period, wo$ch1_norm) * r_ch1^(pk - p_ref)
  new_2  <- interp_within_period(t, per, wo$date, wo$period, wo$ch2_norm) * r_ch2^(pk - p_ref)
  orig_1 <- predict_original(t, pk, "ch1", tr0)
  orig_2 <- predict_original(t, pk, "ch2", tr0)
  time_1 <- interp_within_period(t, per, wo$date, wo$period, wo$ch1_sens)
  time_2 <- interp_within_period(t, per, wo$date, wo$period, wo$ch2_sens)
  # pressure difference from the calibrations either side (how much the correction matters)
  j  <- which(wo$period == per)
  dp <- if (length(j) < 2) NA_real_ else
    pk - approx(as.numeric(wo$date[j]), wo$av_rxn_vessel_pressure[j], xout = as.numeric(t), rule = 2)$y
  tibble(date = t, period = per, p = pk, channel = c("Ch1", "Ch2"),
         measured = c(tk$ch1_sens, tk$ch2_sens),
         new = c(new_1, new_2), original = c(orig_1, orig_2), time_only = c(time_1, time_2),
         dp_neighbours = dp)
}) %>%
  mutate(across(c(new, original, time_only), ~ 100 * (.x / measured - 1), .names = "err_{.col}"))

# D6 plots
method_cols   <- c("New method" = "#0072B2", "Original method" = "#D55E00", "Time only (no pressure)" = "grey55")
method_labels <- c(new = "New method", original = "Original method", time_only = "Time only (no pressure)")

loo_long <- loo %>%
  pivot_longer(starts_with("err_"), names_to = "method", values_to = "err") %>%
  mutate(method = factor(method_labels[sub("err_", "", method)], names(method_cols)))

# summary table
loo_summary <- loo_long %>%
  group_by(channel, method) %>%
  summarise(n = sum(!is.na(err)),
            median_abs_err_pct = median(abs(err), na.rm = TRUE),
            mean_abs_err_pct   = mean(abs(err), na.rm = TRUE),
            pct_within_5pct    = 100 * mean(abs(err) < 5, na.rm = TRUE),
            .groups = "drop")
print(loo_summary)

# D6a. typical error by year
d6a <- loo_long %>%
  group_by(channel, year = year(date), method) %>%
  summarise(med = median(abs(err), na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(factor(year), med, colour = method, group = method)) +
  geom_line(linewidth = 0.6) + geom_point(size = 2.5) +
  facet_wrap(~channel) +
  scale_colour_manual(values = method_cols) +
  labs(x = NULL, y = "Median |error| (%)", colour = NULL,
       title = "D6a. How well each method predicts a calibration it hasn't seen",
       subtitle = "Each calibration left out in turn and predicted from the others; median error per year") +
  theme_bw() + theme(legend.position = "top")
print(d6a)

# D6b. error against the pressure change from the neighbouring calibrations
d6b <- loo_long %>%
  filter(!is.na(dp_neighbours)) %>%
  mutate(dp_bin = cut(abs(dp_neighbours), c(0, 0.5, 1, 2, 4, 8, 40), include.lowest = TRUE,
                      labels = c("0-0.5", "0.5-1", "1-2", "2-4", "4-8", ">8"))) %>%
  group_by(channel, dp_bin, method) %>%
  summarise(n = n(), med = median(abs(err), na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(dp_bin, med, colour = method, group = method)) +
  geom_line(linewidth = 0.6) + geom_point(size = 2.5) +
  facet_wrap(~channel) +
  scale_y_log10() +
  scale_colour_manual(values = method_cols) +
  labs(x = "Pressure difference from the neighbouring calibrations", y = "Median |error| (%, log scale)", colour = NULL,
       title = "D6b. Error against the pressure change from the neighbouring calibrations") +
  theme_bw() + theme(legend.position = "top")
print(d6b)

# D6c. every calibration's error through time (new vs original)
d6c <- loo_long %>%
  filter(method != "Time only (no pressure)") %>%
  mutate(err_plot = pmax(pmin(err, 50), -50)) %>%      # squash beyond +/-50% so the scale stays readable
  ggplot(aes(date, err_plot, colour = method)) +
  geom_hline(yintercept = 0, colour = "grey60") +
  geom_point(size = 0.8, alpha = 0.7) +
  facet_grid(method ~ channel) +
  scale_colour_manual(values = method_cols, guide = "none") +
  labs(x = NULL, y = "Prediction error (%, capped at +/-50)",
       title = "D6c. Leave-one-out error for every calibration") +
  theme_bw()
print(d6c)
