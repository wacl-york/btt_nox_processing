# =====================================================================
# check_1Hz_windows.R
#
# Quick-look plots comparing the old (per-period pressure regression) and new (pressure-normalised
# interpolation) 1 Hz sensitivities for any list of date windows.
#   - calibrations are read once
#   - plot_window() makes one figure for one window
#   - the windows table at the bottom lists every window to check; add a row to add a plot
# Windows up to `max_days_1hz` long are plotted at 1 Hz; longer windows are binned to `bin_size`.
# =====================================================================
library(tidyverse)
library(arrow)

# ---- PATHS ----
old_root     <- "/mnt/longship/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data/"
new_root     <- "/mnt/longship/projects/chem-bttao-2026/btt_processing/processing/1Hz_cal_data_v2/"
old_cal_root <- "/mnt/longship/projects/chem-cmde-2019/btt_processing/processing/calibration_data/"
plot_dir     <- "plots/1Hz_checks"           # set to NULL to only print the plots

# ---- SETTINGS ----
max_days_1hz <- 3              # windows longer than this are binned
bin_size     <- "10 minute"

# cylinder correction (as in process_calibrations_2_2.R); TRUE divides it out of the new values
undo_cyl <- TRUE
cyl_on   <- as.POSIXct("2025-02-03 00:00:00", tz = "UTC")
cyl_off  <- as.POSIXct("2026-08-12 08:00:00", tz = "UTC")
cyl_a    <- 0.0595
cyl_b    <- 0.2972
cyl_factor <- function(d) {
  yrs <- as.numeric(difftime(d, cyl_on, units = "days")) / 365.25
  ifelse(d >= cyl_on & d < cyl_off, 1 + cyl_a + cyl_b * yrs, 1)
}

line_cols <- c("Old: pressure model"               = "#B8963E",
               "Old: time interpolation"           = "#E0C98A",
               "Pressure-normalised interpolation" = "#0072B2")
panel_levels <- c("Ch1 sensitivity", "Ch2 sensitivity", "Rxn vessel pressure")


# ==== 1. CALIBRATIONS (read once) ====
cal_old <- list.files(old_cal_root, pattern = "^coefficients\\.csv$", recursive = TRUE, full.names = TRUE) |>
  map_df(read_csv, show_col_types = FALSE) |>
  mutate(date = as_datetime(date, tz = "UTC")) |>
  filter(!no_cal_flow < 5, !ch1_sens < 1, !ch2_sens < 1, ce < 1, ce > 0,
         !date %in% as_datetime(c("2025-10-17 12:00:00", "2021-01-14 09:00:00",
                                  "2021-01-12 21:00:00"), tz = "UTC")) |>
  transmute(date, ch1 = ch1_sens, ch2 = ch2_sens, pressure = av_rxn_vessel_pressure, source = "Old")

cal_new <- read_csv(file.path(new_root, "calibrations_used.csv"), show_col_types = FALSE) |>
  mutate(date = as_datetime(date, tz = "UTC")) |>
  transmute(date, ch1 = ch1_sens, ch2 = ch2_sens, pressure = av_rxn_vessel_pressure, source = "New")

if (undo_cyl) cal_new <- cal_new |> mutate(ch1 = ch1 / cyl_factor(date), ch2 = ch2 / cyl_factor(date))

cals <- bind_rows(cal_old, cal_new)


# ==== 2. HELPERS ====
# monthly parquet files that overlap [from, to]
month_files <- function(root, from, to) {
  months <- format(seq(floor_date(from, "month"), floor_date(to, "month"), by = "month"), "%Y_%m")
  files  <- list.files(root, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
  files[str_detect(basename(files), paste0("param_(", paste(months, collapse = "|"), ")\\.parquet$"))]
}

# 1 Hz data for one window
read_1hz <- function(root, from, to, cols) {
  open_dataset(month_files(root, from, to), format = "parquet") |>
    filter(datetime >= from, datetime < to) |>
    select(all_of(c("datetime", cols))) |>
    collect()
}

# hourly values of the new method (written next to each monthly parquet by the 1 Hz script)
read_hourly <- function(from, to) {
  month_files(new_root, from, to) |>
    sub(pattern = "\\.parquet$", replacement = "_hourly_sens.csv") |>
    keep(file.exists) |>
    map_df(read_csv, show_col_types = FALSE) |>
    mutate(hour = as_datetime(hour, tz = "UTC"), mid = as_datetime(mid, tz = "UTC")) |>
    filter(mid >= from, mid < to)
}


# ==== 3. ONE FIGURE FOR ONE WINDOW ====
plot_window <- function(from, to, label = NULL) {
  from <- as.POSIXct(from, tz = "UTC")
  to   <- as.POSIXct(to,   tz = "UTC")
  binned <- as.numeric(difftime(to, from, units = "days")) > max_days_1hz
  
  old <- read_1hz(old_root, from, to, c("ch1_sens", "ch2_sens", "ch1_sens_raw", "av_rxn_vessel_pressure")) |>
    mutate(model = !is.na(ch1_sens_raw))
  new <- read_1hz(new_root, from, to, c("ch1_sens", "ch2_sens", "av_rxn_vessel_pressure"))
  hourly <- read_hourly(from, to)
  
  if (undo_cyl) {
    new    <- new    |> mutate(ch1_sens = ch1_sens / cyl_factor(datetime), ch2_sens = ch2_sens / cyl_factor(datetime))
    hourly <- hourly |> mutate(ch1_sens = ch1_sens / cyl_factor(mid),      ch2_sens = ch2_sens / cyl_factor(mid))
  }
  
  # long windows: mean of each bin (old branch = the one used for most of the bin)
  if (binned) {
    bin_it <- function(d) d |>
      mutate(datetime = floor_date(datetime, bin_size)) |>
      group_by(datetime) |>
      summarise(across(c(ch1_sens, ch2_sens, av_rxn_vessel_pressure), \(x) mean(x, na.rm = TRUE)),
                across(any_of("model"), \(x) mean(x, na.rm = TRUE) > 0.5),
                .groups = "drop")
    old <- bin_it(old)
    new <- bin_it(new)
  }
  
  sens <- bind_rows(
    old |> mutate(method = if_else(model, "Old: pressure model", "Old: time interpolation")),
    new |> mutate(method = "Pressure-normalised interpolation")) |>
    transmute(datetime, method, `Ch1 sensitivity` = ch1_sens, `Ch2 sensitivity` = ch2_sens) |>
    pivot_longer(-c(datetime, method), names_to = "panel", values_to = "value")
  
  pres <- new |> transmute(datetime, panel = "Rxn vessel pressure", value = av_rxn_vessel_pressure)
  
  hourly_sens <- hourly |>
    transmute(mid, `Ch1 sensitivity` = ch1_sens, `Ch2 sensitivity` = ch2_sens) |>
    pivot_longer(-mid, names_to = "panel", values_to = "value")
  
  hourly_pres <- hourly |> transmute(hour, end = hour + hours(1), panel = "Rxn vessel pressure", value = p_hour)
  
  cal_pts <- cals |>
    filter(date >= from, date < to) |>
    transmute(date, source, `Ch1 sensitivity` = ch1, `Ch2 sensitivity` = ch2, `Rxn vessel pressure` = pressure) |>
    pivot_longer(-c(date, source), names_to = "panel", values_to = "value")
  
  fp <- function(d) d |> mutate(panel = factor(panel, panel_levels))
  pt <- if (binned) 0.6 else 0.3
  
  p <- ggplot() +
    geom_point(data = fp(pres), aes(datetime, value), size = pt, alpha = 0.4, colour = "grey60", na.rm = TRUE) +
    geom_point(data = fp(sens), aes(datetime, value, colour = method), size = pt, alpha = 0.6, na.rm = TRUE) +
    geom_point(data = fp(cal_pts), aes(date, value, shape = source), fill = "white", colour = "black",
               size = 2.5, stroke = 0.6) +
    facet_wrap(~panel, ncol = 1, scales = "free_y", strip.position = "left") +
    scale_colour_manual(values = line_cols, name = NULL) +
    scale_shape_manual(values = c("Old" = 21, "New" = 24), name = "Calibrations") +
    labs(x = NULL, y = NULL, title = label,
         subtitle = paste0(format(from, "%d %b %Y %H:%M"), " to ", format(to, "%d %b %Y %H:%M"),
                           if (binned) paste0("  (", bin_size, " means)") else "  (1 Hz)",
                           if (undo_cyl && from < cyl_off && to > cyl_on) "  - cylinder correction divided out" else "")) +
    theme_bw(base_size = 12) +
    theme(legend.position = "top", strip.placement = "outside", strip.background = element_blank()) +
    guides(colour = guide_legend(override.aes = list(size = 3, alpha = 1)))
  
  # short windows: also show the new method's hourly values and hourly median pressure
  if (!binned) p <- p +
    geom_segment(data = fp(hourly_pres), aes(x = hour, xend = end, y = value, yend = value),
                 colour = "#0072B2", linewidth = 1) +
    geom_point(data = fp(hourly_sens), aes(mid, value), shape = 21, fill = "#0072B2", colour = "white",
               size = 2.2, stroke = 0.6, na.rm = TRUE)
  
  p
}


# ==== 4. WINDOWS TO CHECK (add a row for each plot) ====
windows <- tribble(
  ~label,                         ~from,                 ~to,
  "Period 13, whole month",       "2025-12-30",          "2026-01-31",
  "Pressure excursion",           "2025-12-30",          "2026-01-03",
  "Branch switching (old)",       "2026-01-26",          "2026-01-27",
  "Feb 2026",                     "2026-02-09",          "2026-03-01",
  
)

windows <- tribble(
  ~label,                         ~from,                 ~to,
  "Summer 2021",                  "2021-06-01",          "2021-07-01",
  "Winter 2022, two days",        "2022-01-10",          "2022-01-12",
  "2023 whole year",              "2023-01-01",          "2024-01-01",
  "Across new year 2022/23",      "2022-12-20",          "2023-01-10",
  "2024",                         "2024-03-30",          "2024-04-30"
)

if (!is.null(plot_dir)) dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

pwalk(windows, function(label, from, to) {
  message("Plotting: ", label)
  p <- plot_window(from, to, label)
  print(p)
  if (!is.null(plot_dir))
    ggsave(file.path(plot_dir, paste0(str_remove_all(str_replace_all(tolower(label), "[^a-z0-9]+", "_"), "^_|_$"), ".png")),
           p, width = 12, height = 8, dpi = 150)
})

# For a one-off look without editing the table:
# plot_window("2026-02-20", "2026-02-22", "quick look")



# =====================================================================
# check_pressure_pattern.R
#
# Is the hour-to-hour pattern in the new sensitivities entirely the pressure effect?
#
# For each hour:
#   baseline = calibration sensitivity interpolated linearly in time (what plain time interpolation gives)
#   p_cal    = calibration pressure interpolated the same way
#   dp       = p_hour - p_cal            (how far the hour's pressure is from the calibrations around it)
#   ratio    = new hourly sensitivity / baseline
# If the pattern is only the pressure effect, ratio = r^dp, so on a log axis the points lie on a
# straight line through (0, 1) with slope ln(r). Scatter about that line = something else is going on.
# Only csv files are read (hourly values and calibrations written by the 1 Hz script).
# =====================================================================
library(tidyverse)

new_root <- "/mnt/longship/projects/chem-bttao-2026/btt_processing/processing/1Hz_cal_data_v2/"

# window to check; set both to NULL for the whole record
check_from <- "2022-12-20"
check_to   <- "2023-01-10"


# ==== 1. INPUTS ====
r_tab <- read_csv(file.path(new_root, "pressure_effect_r.csv"), show_col_types = FALSE)
r_val <- setNames(r_tab$r, r_tab$channel)                     # c(ch1 = ..., ch2 = ...)

cals <- read_csv(file.path(new_root, "calibrations_used.csv"), show_col_types = FALSE) |>
  mutate(date = as_datetime(date, tz = "UTC")) |>
  arrange(date)

hourly <- list.files(new_root, pattern = "_hourly_sens\\.csv$", recursive = TRUE, full.names = TRUE) |>
  map_df(read_csv, show_col_types = FALSE,
         col_types = cols(hour = col_datetime(), mid = col_datetime(),        # <<< NEW: same types in every file
                          period = col_character(), .default = col_double())) |>
  mutate(mid = as_datetime(mid, tz = "UTC")) |>
  filter(!is.na(ch1_sens), !is.na(ch2_sens))

if (!is.null(check_from)) hourly <- hourly |>
  filter(mid >= as.POSIXct(check_from, tz = "UTC"), mid < as.POSIXct(check_to, tz = "UTC"))


# ==== 2. BASELINE AND dp FOR EACH HOUR (interpolated within the period, as in the 1 Hz script) ====
interp_in_period <- function(x, x_period, known_x, known_period, known_y) {
  out <- rep(NA_real_, length(x))
  for (p in unique(x_period)) {
    i <- which(x_period == p)
    j <- which(known_period == p & !is.na(known_y))
    if (length(j) == 0) next
    out[i] <- if (length(j) == 1) known_y[j] else
      approx(as.numeric(known_x[j]), known_y[j], xout = as.numeric(x[i]), rule = 2)$y
  }
  out
}

check <- hourly |>
  mutate(p_cal   = interp_in_period(mid, period, cals$date, cals$period, cals$av_rxn_vessel_pressure),
         base_ch1 = interp_in_period(mid, period, cals$date, cals$period, cals$ch1_sens),
         base_ch2 = interp_in_period(mid, period, cals$date, cals$period, cals$ch2_sens),
         dp       = p_hour - p_cal) |>
  transmute(mid, period, p_hour, p_cal, dp,
            ch1 = ch1_sens / base_ch1,
            ch2 = ch2_sens / base_ch2) |>
  pivot_longer(c(ch1, ch2), names_to = "channel", values_to = "ratio") |>
  filter(!is.na(ratio), !is.na(dp))


# ==== 3. HOW WELL DOES r^dp EXPLAIN IT? ====
fit_summary <- check |>
  group_by(channel) |>
  summarise(n_hours   = n(),
            r_used    = r_val[first(channel)],
            r_fitted  = exp(coef(lm(log(ratio) ~ 0 + dp))[["dp"]]),     # slope through (0, 1)
            resid_pct = 100 * sd(log(ratio) - dp * log(r_used)),        # scatter about r^dp, in %
            max_pct   = 100 * max(abs(log(ratio) - dp * log(r_used))),
            .groups = "drop")
print(fit_summary)


# ==== 4. FIGURE ====
dp_line <- check |>
  group_by(channel) |>
  reframe(dp = seq(min(dp), max(dp), length.out = 50)) |>
  mutate(ratio = r_val[channel]^dp)

labels <- fit_summary |>
  mutate(label = sprintf("r used = %.3f\nr fitted = %.3f\nscatter = %.2f%%", r_used, r_fitted, resid_pct))

fig_check <- ggplot(check, aes(dp, ratio)) +
  geom_hline(yintercept = 1, colour = "grey60") +
  geom_vline(xintercept = 0, colour = "grey60") +
  geom_point(size = 1, alpha = 0.5, colour = "#0072B2") +
  geom_line(data = dp_line, colour = "black", linetype = "dashed") +
  geom_text(data = labels, aes(x = Inf, y = Inf, label = label),
            hjust = 1.1, vjust = 1.2, size = 3.5, inherit.aes = FALSE) +
  facet_wrap(~channel, labeller = as_labeller(c(ch1 = "Channel 1 (NO)", ch2 = "Channel 2 (NO2)"))) +
  scale_y_log10() +
  labs(x = "Hour pressure - interpolated calibration pressure",
       y = "New hourly sensitivity / time-interpolated calibrations (log)",
       caption = "Dashed line = r^dp (pressure effect alone)") +
  theme_bw(base_size = 12)

print(fig_check)
