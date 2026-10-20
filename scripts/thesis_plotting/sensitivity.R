# library
library(tidyverse)
library(paletteer)

# ==== DATA: calibrations used
cal_run_dir <- "/mnt/longship/projects/chem-bttao-2026/btt_processing/processing/1Hz_cal_data_v2"  
plot_dir    <- "plots/thesis_plots"

to_label <- function(p) paste("Period", str_remove(p, "period"))                            
period_levels <- paste("Period", 1:15)                                                    

cal_used <- read_csv(file.path(cal_run_dir, "calibrations_used.csv"), show_col_types = FALSE) |>   
  mutate(period = factor(to_label(period), levels = period_levels))
cal_outliers <- read_csv(file.path(cal_run_dir, "calibrations_outliers_removed.csv"), show_col_types = FALSE) |> 
  mutate(period = factor(to_label(period), levels = period_levels))
r_values <- read_csv(file.path(cal_run_dir, "pressure_effect_r.csv"), show_col_types = FALSE)      

period_cols <- setNames(rep_len(as.character(paletteer_d("ggthemes::manyeys")), 15), period_levels)   

# cylinder correction (same numbers as process_calibrations_2_2.R), used to show the uncorrected values  

cyl_on  <- ymd_hms("2025-02-03 00:00:00", tz = "UTC")                                    
cyl_off <- ymd_hms("2026-08-12 08:00:00", tz = "UTC")                                   
cyl_a   <- 0.0590                                                                    
cyl_b   <- 0.2972                                                                          

# cylinder changes, drawn as red lines (add any others, e.g. "2021-09-01")                
cyl_changes <- ymd_hms(c("2025-02-03 00:00:00", "2026-08-12 08:00:00"), tz = "UTC")      


# ==== FIGURE 1: sensitivities and pressure through time ====

to_long <- function(d) d |>
  rename("Channel 1 Sensitivity" = ch1_sens,
         "Channel 2 Sensitivity" = ch2_sens,
         "Avg reaction vessel pressure" = av_rxn_vessel_pressure) |>
  pivot_longer(cols = c("Channel 1 Sensitivity", "Channel 2 Sensitivity", "Avg reaction vessel pressure"),
               names_to = "type", values_to = "counts") |>
  mutate(type = factor(type, levels = c("Channel 1 Sensitivity", "Channel 2 Sensitivity", "Avg reaction vessel pressure")))

plot_data    <- to_long(cal_used)
plot_removed <- to_long(cal_outliers)  

# sensitivities before the cylinder correction (only the corrected period differs)     
plot_uncorrected <- cal_used |>                                                            
  filter(date >= cyl_on, date < cyl_off) |>
  mutate(factor   = 1 + cyl_a + cyl_b * as.numeric(difftime(date, cyl_on, units = "days")) / 365.25,
         ch1_sens = ch1_sens / factor,
         ch2_sens = ch2_sens / factor) |>
  to_long() |>
  filter(type != "Avg reaction vessel pressure")                                          

period_lines <- cal_used |>
  distinct(date, period) |>
  arrange(date) |>
  filter(period != lag(period)) |>
  mutate(short_label = str_remove(period, "Period "),
         lab_hjust   = if_else(short_label == "14", 1.5, -0.5))   

sens_pressure <- ggplot(plot_data, aes(date, counts, color = period)) +
  geom_point(data = plot_uncorrected, colour = "grey75", size = 1) +                      
  geom_vline(xintercept = as.numeric(cyl_changes), colour = "#A43E36", linewidth = 0.6) +     
  geom_point() +
  geom_point(data = plot_removed, shape = 4, colour = "black", size = 1.5) +           
  geom_vline(data = period_lines, aes(xintercept = as.numeric(date)),
             linetype = "dotted", colour = "grey40") +
  geom_text(data = period_lines, aes(x = date, y = Inf, label = short_label, hjust = lab_hjust),   
            inherit.aes = FALSE, vjust = 1.5,
            colour = "grey40", size = 2.3, fontface = "bold") +
  facet_wrap(~type, ncol = 1, scales = "free_y") +
  theme_bw() +
  theme(legend.position = "none",
        strip.text = element_text(size = 14),
        axis.title.x = element_text(size = 14),
        axis.title.y = element_text(size = 14),
        axis.text = element_text(size = 12)) +
  labs(x = "Year", y = " ") +
  scale_color_manual(values = period_cols)

print(sens_pressure)
ggsave(path = plot_dir, plot = sens_pressure, device = "png", filename = "sens_presssure_timeseries.png",
       dpi = 300, width = 12, height = 8)


# ==== FIGURE 2: sensitivity against pressure for each period ====
# grey dashed: straight line per period (the original method)
# black:       the single pressure relationship used by the new method, S = S_norm * r^(p - p_ref),  
#              drawn with the median normalised sensitivity of each period
channel      <- "ch1"                                                                     
show_r_curve <- FALSE                                                                      
r_ch    <- r_values$r[r_values$channel == channel]
p_ref   <- r_values$p_ref[1]

rel_data <- cal_used |>
  mutate(sens = .data[[paste0(channel, "_sens")]],
         norm = .data[[paste0(channel, "_norm")]])

r_curves <- rel_data |>                                                                     
  group_by(period) |>
  reframe(p = seq(min(av_rxn_vessel_pressure), max(av_rxn_vessel_pressure), length.out = 50),
          sens = median(norm) * r_ch^(p - p_ref))

sens_press_relationships <- ggplot(rel_data, aes(av_rxn_vessel_pressure, sens, color = period)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE, colour = "grey40", linetype = "dashed",          
              linewidth = 0.6, formula = y ~ x) +
  { if (show_r_curve) geom_line(data = r_curves, aes(p, sens), colour = "black", linewidth = 0.8) } +   
  facet_wrap(~period, scales = "free") +
  theme_bw() +
  theme(legend.position = "none",
        strip.text = element_text(size = 14),
        axis.title.x = element_text(size = 14),
        axis.title.y = element_text(size = 14),
        axis.text = element_text(size = 12)) +
  labs(x = "Avg reaction vessel pressure",
       y = paste(if (channel == "ch1") "Channel 1" else "Channel 2", "sensitivity")) +
  scale_color_manual(values = period_cols)

print(sens_press_relationships)
ggsave(path = plot_dir, plot = sens_press_relationships, device = "png",
       filename = "sens_press_relationships.png", dpi = 300, width = 10, height = 8)


# ==== FIGURES 3-5: THE NEW INTERPOLATION METHOD ====                                      
# Uses the same calibrations as above. Calibration pairs are made from all calibrations
# that passed the basic filters (used + outliers), exactly as in the 1 Hz script.
channel_names <- as_labeller(c(ch1 = "Channel 1", ch2 = "Channel 2"))
thesis_theme  <- theme_bw(base_size = 13)
pair_max_days <- 2.5        # same as the 1 Hz script
pair_min_dp   <- 1          # same as the 1 Hz script
pair_xlim     <- c(-12, 16) # Fig 3 pressure-change axis; leaves out only the start-up pairs in Oct 2020. NULL = show all

r_ch1 <- r_values$r[r_values$channel == "ch1"]
r_ch2 <- r_values$r[r_values$channel == "ch2"]

cal_sens <- bind_rows(cal_used, cal_outliers) |>                                           # all calibrations after the basic filters
  arrange(date)

cal_pairs <- cal_sens |>
  select(date, period, pressure = av_rxn_vessel_pressure, ch1 = ch1_sens, ch2 = ch2_sens) |>
  pivot_longer(c(ch1, ch2), names_to = "channel", values_to = "sens") |>
  group_by(channel, period) |>                                                              # never pair across a period break
  arrange(date, .by_group = TRUE) |>
  mutate(days  = as.numeric(difftime(date, lag(date), units = "days")),
         dp    = pressure - lag(pressure),
         ratio = sens / lag(sens)) |>
  ungroup() |>
  filter(days <= pair_max_days, abs(dp) >= pair_min_dp) |>
  mutate(r_pair = ratio^(1 / dp))

r_by_channel <- cal_pairs |>
  group_by(channel) |>
  summarise(n_pairs = n(), r = median(r_pair), .groups = "drop") |>
  mutate(label = sprintf("r = %.3f\n%.1f%% per unit pressure\n(%d pairs)", r, 100 * (r - 1), n_pairs))
print(r_by_channel |> select(channel, n_pairs, r))                                           # should match pressure_effect_r.csv


# ---- FIGURE 3: change in sensitivity against pressure change for every pair ----
dp_range <- if (is.null(pair_xlim)) range(cal_pairs$dp) else pair_xlim
r_curve <- r_by_channel |>
  crossing(dp = seq(floor(dp_range[1]), ceiling(dp_range[2]), by = 0.1)) |>
  mutate(pct_change = 100 * (r^dp - 1))

in_view  <- between(cal_pairs$dp, dp_range[1], dp_range[2])
y_range  <- range(c(100 * (cal_pairs$ratio[in_view] - 1), r_curve$pct_change))
n_hidden <- sum(!in_view & cal_pairs$channel == "ch1")
pair_labels <- r_by_channel 
  #mutate(label = if (n_hidden > 0) paste0(label, sprintf("\n%d pairs outside axis", n_hidden)) else label)

fig_pairs <- ggplot(cal_pairs, aes(dp, 100 * (ratio - 1))) +
  geom_hline(yintercept = 0, colour = "grey70") +
  geom_vline(xintercept = 0, colour = "grey70") +
  geom_line(data = r_curve, aes(dp, pct_change), linewidth = 1) +
  geom_point(aes(colour = factor(year(date))), size = 2.5, alpha = 0.6) +
  geom_text(data = pair_labels, aes(x = Inf, y = Inf, label = label), hjust = 1.05, vjust = 1.2, size = 3.8) +
  facet_wrap(~channel, labeller = channel_names) +
  coord_cartesian(xlim = dp_range, ylim = y_range) +
  scale_color_manual(values = c("#7C2A52", "#C66526", "#469C76", "#88884D", "#0072B2")) +
  labs(x = paste0("Change in reaction cell pressure between calibrations (< ", pair_max_days, " days apart)"),
       y = "Change in sensitivity (%)", colour = "Year") +
  thesis_theme +
  theme(strip.text             = element_text(size = 14),
        axis.title.x           = element_text(size = 14),
        axis.title.y           = element_text(size = 14),
        axis.text              = element_text(size = 12),
        axis.title.y.right     = element_text(size = 14, color = "black"),
        axis.text.y.right      = element_text(size = 12, color = "black"))

print(fig_pairs)
ggsave(path = "plots/thesis_plots", plot = fig_pairs, device = "png", filename = "sens_pairs.png",
       dpi = 300, width = 10, height = 5)


# ---- FIGURE 4: measured against predicted from the previous calibration ----
pair_pred <- cal_pairs |>
  left_join(select(r_by_channel, channel, r_all = r), by = "channel") |>
  mutate(sens_before = sens / ratio,
         predicted   = sens_before * r_all^dp,                   # previous calibration x r^(pressure change)
         err_r       = 100 * (predicted   / sens - 1),           # error using r
         err_none    = 100 * (sens_before / sens - 1))           # error assuming no change

pair_stats <- pair_pred |>
  group_by(channel) |>
  summarise(med_err_r = median(abs(err_r)), med_err_none = median(abs(err_none)), .groups = "drop") |>
  mutate(label = sprintf("median error %.1f%%\n(%.1f%% without pressure)", med_err_r, med_err_none))

fig_pred <- ggplot(pair_pred, aes(predicted, sens)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  geom_point(aes(colour = factor(year(date))), size = 2) +
  geom_text(data = pair_stats, aes(x = -Inf, y = Inf, label = label), hjust = -0.05, vjust = 1.2, size = 3.8) +
  facet_wrap(~channel, scales = "free", labeller = channel_names) +
  labs(x = "Sensitivity predicted from previous calibration and pressure change",
       y = "Measured sensitivity", colour = "Year") +
  thesis_theme

print(fig_pred)
ggsave(path = plot_dir, plot = fig_pred, device = "png", filename = "sens_pairs_pred.png",
       dpi = 300, width = 10, height = 5)


# ---- FIGURE 5: leave-one-out test (takes 1-2 minutes) ----
# Each calibration is left out in turn and predicted from the others with
#   new:       normalised calibrations interpolated in time within the period, x r^(p - p_ref)
#   original:  per-period straight-line pressure model within the calibrated pressure range,
#              otherwise time interpolation over all calibrations
#   time only: time interpolation within the period, no pressure correction

interp_within_period <- function(x, x_period, known_x, known_period, known_y) {   # as in the 1 Hz script
  out <- rep(NA_real_, length(x))
  for (p in unique(na.omit(x_period))) {
    i <- which(x_period == p)
    j <- which(known_period == p & !is.na(known_y))
    if (length(j) == 0) next
    if (length(j) == 1) { out[i] <- known_y[j]; next }
    out[i] <- approx(as.numeric(known_x[j]), known_y[j], xout = as.numeric(x[i]), rule = 2)$y
  }
  out
}

orig_fit_period <- function(d) case_when(                       # periods the original method fitted lines in
  d <= ymd_hms("2020-10-13 01:00:00") ~ "period1",
  d <  ymd("2021-01-11") ~ "period2",  d < ymd("2021-06-11") ~ "period3",
  d <  ymd("2021-12-31") ~ "period4",  d < ymd("2022-11-01") ~ "period5",
  d <  ymd("2023-10-19") ~ "period6",  d < ymd("2024-02-08") ~ "period7",
  d <  ymd("2024-08-31") ~ "period8",  d < ymd("2025-03-18") ~ "period9",
  d <  ymd("2025-06-05") ~ "period10", d < ymd("2025-11-13") ~ "period11",
  d <  ymd("2025-12-30") ~ "period12", d < ymd("2026-07-16") ~ "period13",
  TRUE ~ "period14")
orig_apply_period <- function(d) case_when(                     # periods the original method applied them in
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

cal_orig <- cal_sens |>                                         # original method only used sensitivities >= 1
  filter(ch1_sens >= 1, ch2_sens >= 1) |>
  mutate(fit_period = orig_fit_period(date))

predict_original <- function(t, p, ch, train) {
  sens  <- train[[paste0(ch, "_sens")]]
  t_int <- approx(as.numeric(train$date), sens, xout = as.numeric(t), rule = 2)$y
  per   <- orig_apply_period(t)
  if (!per %in% orig_lm_periods || p > 40) return(t_int)
  d <- train |> filter(fit_period == per, av_rxn_vessel_pressure < 400, ch1_sens < 10)
  if (nrow(d) < 3 || p < min(d$av_rxn_vessel_pressure) || p > max(d$av_rxn_vessel_pressure)) return(t_int)
  b <- coef(lm(d[[paste0(ch, "_sens")]] ~ d$av_rxn_vessel_pressure))
  b[[1]] + b[[2]] * p
}

loo_cals <- cal_used |> mutate(period = as.character(period)) |> arrange(date)
p_ref    <- r_values$p_ref[1]

loo <- map_dfr(seq_len(nrow(loo_cals)), function(k) {
  tk <- loo_cals[k, ]; t <- tk$date; pk <- tk$av_rxn_vessel_pressure; per <- tk$period
  wo  <- loo_cals[-k, ]
  tr0 <- cal_orig |> filter(date != t)
  new_1  <- interp_within_period(t, per, wo$date, wo$period, wo$ch1_norm) * r_ch1^(pk - p_ref)
  new_2  <- interp_within_period(t, per, wo$date, wo$period, wo$ch2_norm) * r_ch2^(pk - p_ref)
  orig_1 <- predict_original(t, pk, "ch1", tr0)
  orig_2 <- predict_original(t, pk, "ch2", tr0)
  time_1 <- interp_within_period(t, per, wo$date, wo$period, wo$ch1_sens)
  time_2 <- interp_within_period(t, per, wo$date, wo$period, wo$ch2_sens)
  j  <- which(wo$period == per)
  dp <- if (length(j) < 2) NA_real_ else
    pk - approx(as.numeric(wo$date[j]), wo$av_rxn_vessel_pressure[j], xout = as.numeric(t), rule = 2)$y
  tibble(date = t, channel = c("ch1", "ch2"), measured = c(tk$ch1_sens, tk$ch2_sens),
         new = c(new_1, new_2), original = c(orig_1, orig_2), time_only = c(time_1, time_2),
         dp_neighbours = dp)
}) |>
  mutate(across(c(new, original, time_only), ~ 100 * (.x / measured - 1), .names = "err_{.col}"))

method_cols   <- c("New method" = "#0072B2", "Original method" = "#B89F4B", "Time only" = "grey55")
method_labels <- c(new = "New method", original = "Original method", time_only = "Time only")

loo_long <- loo |>
  pivot_longer(starts_with("err_"), names_to = "method", values_to = "err") |>
  mutate(method = factor(method_labels[sub("err_", "", method)], names(method_cols)))

loo_summary <- loo_long |>
  group_by(channel, method) |>
  summarise(n = sum(!is.na(err)),
            median_abs_err_pct = median(abs(err), na.rm = TRUE),
            mean_abs_err_pct   = mean(abs(err), na.rm = TRUE),
            pct_within_5pct    = 100 * mean(abs(err) < 5, na.rm = TRUE),
            .groups = "drop")

fig_loo <- loo_long |>
  filter(!is.na(dp_neighbours)) |>
  mutate(dp_bin = cut(abs(dp_neighbours), c(0, 0.5, 1, 2, 4, 8, 40), include.lowest = TRUE,
                      labels = c("0-0.5", "0.5-1", "1-2", "2-4", "4-8", ">8"))) |>
  group_by(channel, dp_bin, method) |>
  summarise(med = median(abs(err), na.rm = TRUE), .groups = "drop") |>
  ggplot(aes(dp_bin, med, colour = method, group = method)) +
  geom_line(linewidth = 0.7) + geom_point(size = 2.5) +
  facet_wrap(~channel, labeller = channel_names) +
  scale_y_log10() +
  scale_colour_manual(values = method_cols) +
  labs(x = "Pressure difference from the neighbouring calibrations",
       y = "Median absolute error (%, log scale)", colour = NULL) +
  thesis_theme + theme(legend.position = "top")

print(fig_loo)
ggsave(path = "plots/thesis_plots", plot = fig_loo, device = "png", filename = "sens_loo.png",
       dpi = 300, width = 10, height = 5)


loo_binned <- loo_long |>                                                                  # <<< NEW
  filter(!is.na(dp_neighbours)) |>
  mutate(dp_bin = cut(abs(dp_neighbours), c(0, 0.5, 1, 2, 4, 8, 40), include.lowest = TRUE,
                      labels = c("0-0.5", "0.5-1", "1-2", "2-4", "4-8", ">8")))
bin_n <- loo_binned |> filter(channel == "ch1", method == "New method") |> count(dp_bin)  # <<< NEW: calibrations per bin
bin_labels <- setNames(sprintf("%s\n(n = %d)", bin_n$dp_bin, bin_n$n), bin_n$dp_bin)      # <<< NEW

fig_loo <- loo_binned |>                                                                   # <<< CHANGED
  group_by(channel, dp_bin, method) |>
  summarise(med = median(abs(err), na.rm = TRUE), .groups = "drop") |>
  ggplot(aes(dp_bin, med, colour = method, group = method)) +
  geom_line(linewidth = 0.7) + geom_point(size = 2.5) +
  facet_wrap(~channel, labeller = channel_names) +
  scale_x_discrete(labels = bin_labels) +                                                  # <<< NEW
  scale_y_log10() +
  scale_colour_manual(values = method_cols) +
  labs(x = "Pressure difference from the neighbouring calibrations",
       y = "Median absolute error (%, log scale)", colour = NULL) +
  thesis_theme + theme(legend.position = "top")


# ---- FIGURE: prediction error against pressure difference, every calibration ----        # <<< NEW
loo_pts <- loo |>
  filter(channel == "ch1", !is.na(dp_neighbours)) |>
  pivot_longer(starts_with("err_"), names_to = "method", values_to = "err") |>
  mutate(method = factor(method_labels[sub("err_", "", method)], names(method_cols)),
         ratio  = 1 + err / 100)

dp_rng <- range(loo_pts$dp_neighbours)
pressure_only <- tibble(dp = seq(dp_rng[1], dp_rng[2], 0.1), ratio = r_ch1^(-dp),
                        method = factor("Linear interpolation in time", names(method_cols)))   # <<< CHANGED: new name

err_breaks <- c(-75, -50, -20, 0, 25, 100, 300, 700)       # axis labels, in % error

method_cols   <- c("Pressure-normalised interpolation" = "#0072B2",                          # <<< CHANGED
                   "Per-period pressure regression"    = "#B8963E",
                   "Linear interpolation in time"      = "grey55")

method_labels <- c(new       = "Pressure-normalised interpolation",                          # <<< CHANGED
                   original  = "Per-period pressure regression",
                   time_only = "Linear interpolation in time")

fig_err_dp <- ggplot(loo_pts, aes(dp_neighbours, ratio, colour = method)) +
  geom_hline(yintercept = 1, colour = "grey40") +           # ratio 1 = no error
  geom_point(size = 1.4, alpha = 0.6) +
  geom_line(data = pressure_only, aes(dp, ratio), colour = "black", linetype = "dashed") +
  facet_wrap(~method) +
  scale_y_log10(breaks = 1 + err_breaks / 100,
                labels = paste0(ifelse(err_breaks > 0, "+", ""), err_breaks)) +
  scale_colour_manual(values = method_cols, guide = "none") +
  geom_text(data = tibble(method = factor("Linear interpolation in time", names(method_cols)),     # <<< NEW: only in this panel
                          label  = sprintf("Error == 100~(%.3f^{-Delta*italic(p)} - 1)", r_ch1)),
            aes(x = -Inf, y = Inf, label = label), parse = TRUE,
            hjust = -0.05, vjust = 1.4, size = 4, colour = "black", inherit.aes = FALSE) +
  labs(x = "Pressure difference from the neighbouring calibrations",
       y = "Prediction error (%, log scale)") +
  thesis_theme +
  theme(strip.text             = element_text(size = 14),
        axis.title.x           = element_text(size = 14),
        axis.title.y           = element_text(size = 14),
        axis.text              = element_text(size = 12),
        axis.title.y.right     = element_text(size = 14, color = "black"),
        axis.text.y.right      = element_text(size = 12, color = "black"))
  

print(fig_err_dp)
ggsave(path = plot_dir, plot = fig_err_dp, device = "png", filename = "sens_loo_err_dp.png",
       dpi = 300, width = 11, height = 4.5)


# ---- NUMBERS FOR THE TEXT ----
cat("\n==================== NUMBERS FOR THE TEXT ====================\n")
cat(sprintf("Calibrations: %d after basic filters, %d outliers removed, %d used\n",
            nrow(cal_sens), nrow(cal_outliers), nrow(cal_used)))
cat(sprintf("Pairs: %d per channel (<= %.1f days apart, |dp| >= %g, same period)\n",
            sum(cal_pairs$channel == "ch1"), pair_max_days, pair_min_dp))
print(r_by_channel |> select(channel, n_pairs, r) |> mutate(pct_per_unit = 100 * (r - 1)))
cat("\nPair predictions (Fig 4): median |error| with r vs assuming no change\n")
print(pair_stats |> select(channel, med_err_r, med_err_none))
cat("\nLeave-one-out (Fig 5):\n")
print(loo_summary, n = Inf)
cat("==============================================================\n")

