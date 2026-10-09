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

write_csv(hourly_all, "data/data/hourly_pressure_all.csv")    # read this back next time instead of re-running

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
