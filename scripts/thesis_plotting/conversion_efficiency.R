

# looking for dipped and spikey calibrations 
cal_dir   <- "/mnt/longship/projects/chem-bttao-2026/btt_processing/processing/calibration_data/"

cal_files <- tibble(file = list.files(cal_dir, pattern = "^coefficients(_2)?\\.csv$",                
                                      recursive = TRUE, full.names = TRUE)) %>%                      
  mutate(folder = dirname(file), corrected = str_detect(basename(file), "_2")) %>%                  
  group_by(folder) %>%                                                                             
  filter(corrected | !any(corrected)) %>%                  # _2 if the folder has one              
  pull(file)                  

cal_raw <- cal_files |> 
  map_df(read_csv, show_col_types = FALSE) %>%
  mutate(date = as_datetime(date, tz = "UTC")) %>%
  filter(!no_cal_flow < 5) %>%   # no-cal flow went below 5 (set at 10 sccm)
  filter(!date %in% as_datetime(c("2025-10-17 12:00:00",   # dodgy cal after restart
                                  "2021-01-14 09:00:00",   # inlet pressure messed up
                                  "2021-01-12 21:00:00"),  # inlet pressure messed up
                                tz = "UTC")) %>%
  arrange(date) %>%
  distinct(date, .keep_all = TRUE)   

cal_coefficients <- cal_raw %>%
  filter(ch1_sens >= 1, ch2_sens >= 1)

cal_coeff_monthly = cal_coefficients |> 
  mutate(date2 = as.Date(date), 
         month = floor_date(date2, "month")) |> 
  group_by(month) |> 
  summarise(total_obs = length(unique(date)))

cal_flag1 = cal_coefficients |> 
  filter(cal_flag1 == 1)

length(unique(cal_flag1$date))/length(unique(cal_coefficients$date))*100

cal_flag1 |> 
  mutate(date2 = as.Date(date), 
         month = floor_date(date2, "month")) |> 
  group_by(month) |> 
  summarise(n_obs = length(unique(date))) |> 
  left_join(cal_coeff_monthly) |> 
  mutate(percent = n_obs/total_obs*100) |> 
  arrange(month) |> 
  ggplot(aes(month, percent)) +
  geom_line()
geom_bar(stat = "count", position = "stack")


cal_flag2 = cal_coefficients |> 
  filter(cal_flag2 == 1)

length(unique(cal_flag2$date))/length(unique(cal_coefficients$date))*100


cal_flag3 = cal_coefficients |> 
  filter(cal_flag3 == 1)

length(unique(cal_flag3$date))/length(unique(cal_coefficients$date))*100

check = cal_flag1 |> 
  mutate(date2 = as.Date(date), 
         month = floor_date(date2, "month")) |> 
  group_by(month) |> 
  summarise(n_obs = length(unique(date))) 

check_all = left_join(cal_coeff_monthly, check) |> 
  mutate(percent = n_obs/total_obs*100) |> 
  arrange(month) 
ggplot(aes(month, percent)) +
  geom_line()

check = cal_coefficients |> 
  mutate(date2 = as.Date(date),
         month = floor_date(date2, "month")) |> 
  group_by(month) |> 
  summarise(
    n_obs   = sum(cal_flag1 == 1, na.rm = TRUE),
    total   = n_distinct(date),
    percent = n_obs / total * 100,
    .groups = "drop"
  )

cal_flag2 = cal_coefficients |> 
  filter(cal_flag2 == 1)

length(unique(cal_flag2$date))/length(unique(cal_coefficients$date))*100

cal_flag2 |> 
  mutate(date2 = as.Date(date), 
         week = floor_date(date2, "week")) |> 
  group_by(week) |> 
  summarise(n_obs = length(unique(date))) |> 
  left_join(cal_coeff_monthly) |> 
  mutate(percent = n_obs/total_obs*100) |> 
  arrange(week) |> 
  ggplot(aes(week, percent)) +
  geom_line()

check2 = cal_coefficients |> 
  mutate(date2 = as.Date(date),
         month = floor_date(date2, "month")) |> 
  group_by(month) |> 
  summarise(
    n_obs   = sum(cal_flag3 == 1, na.rm = TRUE),
    total   = n_distinct(date),
    percent = n_obs / total * 100,
    .groups = "drop"
  )

cal_flag3 = cal_coefficients |> 
  filter(cal_flag3 == 1)

length(unique(cal_flag3$date))/length(unique(cal_coefficients$date))*100

cal_flag3 |> 
  mutate(date2 = as.Date(date), 
         week = floor_date(date2, "week")) |> 
  group_by(week) |> 
  summarise(n_obs = length(unique(date))) |> 
  left_join(cal_coeff_monthly) |> 
  mutate(percent = n_obs/total_obs*100) |> 
  arrange(week) |> 
  ggplot(aes(week, percent)) +
  geom_line()


cal_coefficients |> 
  filter(ce > 0, 
         ce < 1) |> 
  ggplot(aes(date, ce, color = cal_flag2)) +
  geom_point()+
  geom_line()

cal_coefficients |> 
  filter(cal_flag1 == 1) |>                         
  summarise(
    total_flag1    = n(),
    both_flagged   = sum(cal_flag2 == 1, na.rm = TRUE),
    pct_coinciding = (both_flagged / total_flag1) * 100
  )


check2 = cal_coefficients |> 
  mutate(date2 = as.Date(date),
         month = floor_date(date2, "month")) |> 
  group_by(month) |> 
  summarise(
    n_obs   = sum(cal_flag1 == 1, na.rm = TRUE),
    total   = n_distinct(date),
    percent = n_obs / total * 100,
    .groups = "drop"
  ) |> 
  filter(month >= "2022-09-01") |> 
  filter(month <= "2025-09-01")

median(check2$percent)

# Make the figure which shows interpolation 


# =====================================================================
# ce_figure.R
#
# Conversion efficiency (CE) figure for the thesis.
# Repeats the CE steps of the 1 Hz script (section 6 of build_1Hz_calibration_parquet_2_4_new.R)
# and shows, for every calibration, whether its CE was used, removed (and why), or replaced by
# interpolation, together with the final CE series used in the processing.
# Keep the filters and interpolation windows the same as in the 1 Hz script.
# =====================================================================
library(tidyverse)


# interpolation windows (same as the 1 Hz script)
interp_ranges <- list(
  c("2022-09-07", "2024-02-20"),
  c("2024-04-20", "2025-04-20"),
  c("2025-06-13", "2025-10-17"),
  c("2026-01-02", "2026-03-01")
) |> lapply(as.POSIXct, tz = "UTC")

ce_range <- c(0.45, 0.7)   # final CEs outside this range are removed (same as the 1 Hz script)


# ==== 1. READ CALIBRATIONS (as in the 1 Hz script) ====
cal_files <- tibble(file = list.files(cal_dir, pattern = "^coefficients(_2)?\\.csv$",
                                      recursive = TRUE, full.names = TRUE)) |>
  mutate(folder = dirname(file), corrected = str_detect(basename(file), "_2")) |>
  group_by(folder) |>
  filter(corrected | !any(corrected)) |>
  pull(file)

cal_raw <- cal_files |>
  map_df(read_csv, show_col_types = FALSE) |>
  mutate(date = as_datetime(date, tz = "UTC")) |>
  filter(!no_cal_flow < 5) |>
  filter(!date %in% as_datetime(c("2025-10-17 12:00:00", "2021-01-14 09:00:00",
                                  "2021-01-12 21:00:00"), tz = "UTC")) |>
  arrange(date) |>
  distinct(date, .keep_all = TRUE)


# ==== 2. CE STEPS (as in the 1 Hz script, section 6) ====
cal_coefficients <- cal_raw |> filter(ch1_sens >= 1, ch2_sens >= 1)

in_window <- function(d) Reduce(`|`, lapply(interp_ranges, function(r) d >= r[1] & d <= r[2]))

interpolate_ce <- cal_coefficients |>
  filter(ce_zero > 0, ce_zero < 1) |>
  arrange(date) |>
  mutate(interp_status = ifelse(in_window(date), "yes", "no")) |>
  filter(!(interp_status == "no" & cal_flag1 == 1)) |>
  filter(!(interp_status == "no" & inlet_pressure < 199)) |>
  filter(!(interp_status == "no" & inlet_pressure > 300)) |>
  mutate(ce_interpolated = {
    ce_out <- ce_zero
    for (r in interp_ranges) {
      inside_idx <- which(date >= r[1] & date <= r[2])
      before_idx <- which(date < r[1])
      after_idx  <- which(date > r[2])
      if (length(inside_idx) == 0) next
      start_val  <- if (length(before_idx) > 0) median(tail(ce_zero[before_idx], 15), na.rm = TRUE) else ce_zero[inside_idx[1]]
      start_date <- if (length(before_idx) > 0) date[max(before_idx)] else date[inside_idx[1]]
      end_val    <- if (length(after_idx) > 0) ce_zero[min(after_idx)] else start_val
      end_date   <- if (length(after_idx) > 0) date[min(after_idx)] else date[inside_idx[length(inside_idx)]]
      ce_out[inside_idx] <- approx(as.numeric(c(start_date, end_date)), c(start_val, end_val),
                                   xout = as.numeric(date[inside_idx]), rule = 2)$y
    }
    ce_out
  }) |>
  filter(ce_interpolated > ce_range[1], ce_interpolated < ce_range[2])

ce_final <- interpolate_ce |> select(date, ce = ce_interpolated, interp_status) |> arrange(date)


cal_all <- cal_files |>                                                      
  map_df(read_csv, show_col_types = FALSE) |>
  mutate(date = as_datetime(date, tz = "UTC")) |>
  arrange(date) |>
  distinct(date, .keep_all = TRUE)

excluded_dates <- as_datetime(c("2025-10-17 12:00:00", "2021-01-14 09:00:00", 
                                "2021-01-12 21:00:00"), tz = "UTC")

cal_raw <- cal_all |>                                                         
  filter(!no_cal_flow < 5) |>
  filter(!date %in% excluded_dates)

# ==== 3. FLAGS AND TREATMENT OF EACH CALIBRATION ====                                  
# Colour (status): flags 1, 2, 3 in that order (Table tab:flags, cal_flag1-3 from process_calibrations);
# calibrations with none of these flags that were removed are coloured by the first check that removed them.
# Shape (treatment): what the 1 Hz script did with the calibration.
status_levels <- c("No flag", "Flag 1: dipped", "Flag 2: low cal flow", "Flag 3: spikey", 
                   "Removed: cal flow < 5 sccm", "Removed: by hand", "Removed: sensitivity < 1",
                   "Removed: inlet pressure", "Removed: CE outside 0.45-0.7")

cal_plot <- cal_all |>
  mutate(
    treatment = case_when(
      in_window(date)          ~ "Replaced by interpolation",
      date %in% ce_final$date  ~ "Used",
      TRUE                     ~ "Removed"),
    status = case_when(                                                                  
      cal_flag1 == 1                                   ~ "Flag 1: dipped",
      cal_flag2 == 1                                   ~ "Flag 2: low cal flow",
      cal_flag3 == 1                                   ~ "Flag 3: spikey",
      treatment != "Removed"                           ~ "No flag",
      # removed, no flag 1-3: first check in the 1 Hz script that removed it
      no_cal_flow < 5                                  ~ "Removed: cal flow < 5 sccm",
      date %in% excluded_dates                         ~ "Removed: by hand",
      ch1_sens < 1 | ch2_sens < 1                      ~ "Removed: sensitivity < 1",
      inlet_pressure < 199 | inlet_pressure > 300      ~ "Removed: inlet pressure",
      TRUE                                             ~ "Removed: CE outside 0.45-0.7"),
    status    = factor(status, status_levels),
    treatment = factor(treatment, c("Used", "Replaced by interpolation", "Removed")))

# numbers for the text
cal_plot |> summarise(n = n(), flag1_pct = 100 * mean(cal_flag1 == 1, na.rm = TRUE),
                      flag2_pct = 100 * mean(cal_flag2 == 1, na.rm = TRUE),
                      flag3_pct = 100 * mean(cal_flag3 == 1, na.rm = TRUE)) |> print()
print(count(cal_plot, treatment, status), n = Inf)                                       # <<< CHANGED


# ==== 4. FIGURE ====
# Top panel: CE of every calibration. Bottom strip: a tick for each calibration with flag 1, 2 or 3.
# Both are facets of one plot, so they share the time axis.
ylims <- c(0, 0.9)
message(sum(cal_plot$ce_zero < ylims[1] | cal_plot$ce_zero > ylims[2], na.rm = TRUE),
        " calibrations off the CE axis (not drawn)")

flag_rows <- c("Flag 1" = -0.05, "Flag 2" = -0.15, "Flag 3" = -0.25)   # y positions in the strip

ce_pts <- cal_plot |>
  filter(between(ce_zero, ylims[1], ylims[2])) |>
  mutate(panel = "ce")

flag_ticks <- cal_plot |>
  select(date, cal_flag1, cal_flag2, cal_flag3) |>
  pivot_longer(-date, names_to = "flag", values_to = "on") |>
  filter(on == 1) |>
  mutate(flag  = recode(flag, cal_flag1 = "Flag 1", cal_flag2 = "Flag 2", cal_flag3 = "Flag 3"),
         y     = flag_rows[flag],
         status = factor(recode(flag, "Flag 1" = "Flag 1: dipped", "Flag 2" = "Flag 2: low cal flow",   
                                "Flag 3" = "Flag 3: spikey"), status_levels),
         panel = "flags")

windows <- tibble(start = do.call(c, lapply(interp_ranges, `[`, 1)),
                  end   = do.call(c, lapply(interp_ranges, `[`, 2)))

# CE applied in the processing, split into measured and interpolated runs.
# Each interpolated run also takes the measured point either side, so the dashed line joins up.
ce_runs <- ce_final |> mutate(run = consecutive_id(interp_status))
ce_lines <- bind_rows(
  ce_runs |> filter(interp_status == "no")  |> mutate(part = "Measured CE",     grp = run),
  ce_runs |> filter(interp_status == "yes") |> mutate(part = "Interpolated CE", grp = run),
  ce_runs |> filter(interp_status == "no", lead(interp_status) == "yes") |> mutate(part = "Interpolated CE", grp = run + 1),
  ce_runs |> filter(interp_status == "no", lag(interp_status)  == "yes") |> mutate(part = "Interpolated CE", grp = run - 1)
) |> arrange(date) |> mutate(panel = "ce")

# invisible points that set the y range of each panel (the strip is 1/3 the height of the CE panel)
panel_limits <- tibble(panel = c("ce", "ce", "flags", "flags"),
                       date  = min(cal_plot$date), y = c(ylims, -0.27, -0.04))

status_cols <- c("No flag"                      = "grey30",                              # <<< CHANGED
                 "Flag 1: dipped"               = "#CC79A7",
                 "Flag 2: low cal flow"         = "#E69F00",
                 "Flag 3: spikey"                = "#009E73",
                 "Removed: cal flow < 5 sccm"   = "#D55E00",
                 "Removed: by hand"             = "grey55",
                 "Removed: sensitivity < 1"     = "#8C510A",
                 "Removed: inlet pressure"      = "#6A3D9A",
                 "Removed: CE outside 0.45-0.7" = "#56B4E9")
treatment_shapes <- c("Used" = 16, "Replaced by interpolation" = 1, "Removed" = 4)

fig_ce <- ggplot() +
  geom_rect(data = windows, aes(xmin = start, xmax = end, ymin = -Inf, ymax = Inf,
                                fill = "Interpolation window")) +
  geom_blank(data = panel_limits, aes(date, y)) +
  geom_point(data = ce_pts, aes(date, ce_zero, colour = status, shape = treatment),      # <<< CHANGED
             size = 1.6, alpha = 0.8) +
  geom_line(data = ce_lines, aes(date, ce, group = interaction(part, grp), linetype = part),
            colour = "#0072B2", linewidth = 0.8) +
  geom_point(data = flag_ticks, aes(date, y, colour = status), shape = "|", size = 3,    # <<< CHANGED
             show.legend = FALSE) +
  facet_grid(panel ~ ., scales = "free_y", space = "free_y") +
  scale_y_continuous(breaks = c(flag_rows, 0, 0.25, 0.5, 0.75),
                     labels = c(names(flag_rows), "0.00", "0.25", "0.50", "0.75"),
                     expand = expansion(add = 0.03)) +
  scale_fill_manual(values = c("Interpolation window" = "grey90"), name = NULL) +
  scale_colour_manual(values = status_cols, drop = TRUE, name = "Flag / reason removed") +  # <<< CHANGED
  scale_shape_manual(values = treatment_shapes, drop = FALSE, name = "Treatment") +
  scale_linetype_manual(values = c("Measured CE" = "solid", "Interpolated CE" = "42"),
                        breaks = c("Measured CE", "Interpolated CE"), name = "CE applied") +
  scale_x_datetime(date_breaks = "1 year", date_labels = "%Y") +
  labs(x = NULL, y = "Conversion efficiency") +
  theme_bw() +
  theme(strip.background     = element_blank(),
        strip.text           = element_blank(),
        panel.spacing.y      = unit(4, "pt"),
        legend.position      = "right",
        legend.justification = "top",
        legend.title         = element_text(face = "bold", size = 11),
        legend.text          = element_text(size = 10),
        legend.key.height    = unit(14, "pt"),
        legend.spacing.y     = unit(10, "pt")) +
  guides(colour   = guide_legend(order = 1, override.aes = list(shape = 15, size = 4, alpha = 1)),
         shape    = guide_legend(order = 2, override.aes = list(size = 2.5, alpha = 1)),
         linetype = guide_legend(order = 3, override.aes = list(linewidth = 0.8), keywidth = unit(28, "pt")),
         fill     = guide_legend(order = 4))

print(fig_ce)

ggsave(path = "plots/thesis_plots/", plot = fig_ce, device = "png", filename = "ce_timeseries.png",
       dpi = 300, width = 13, height = 6)
