# library

library(tidyverse)
library(purrr)
library(paletteer)
library(plotly)
library(arrow)
library(zoo)

# checking sensitivities 


coef_files = list.files("/mnt/longship/projects/chem-cmde-2019/btt_processing/processing/calibration_data/", 
                        pattern = "coefficients.csv", 
                        recursive = T, 
                        full.names = T)


coeff_data = map_df(coef_files, read_csv)

write_csv(coeff_data, "data/data/coeff_data.csv")


sens_files = list.files("/mnt/longship/projects/chem-cmde-2019/btt_processing/processing/calibration_data/", 
                        pattern = "sensitivity_params.csv", 
                        recursive = T, 
                        full.names = T)

sens_data = map_df(sens_files, read_csv)


# raw sens 

coeff_data |>
  filter(ch1_sens < 10) |> 
  pivot_longer(cols = c(ch1_sens, ch2_sens), 
               values_to = "sens", names_to = "channel") |> 
  ggplot(aes(date, sens, color = channel)) +
  geom_line(linewidth = 1) +
  theme_minimal()
  


# sensitivity and pressure plots

# check cals 


cal_coefficients <- coef_files %>%
  map_df(read_csv) %>% 
  filter(!no_cal_flow < 5) %>% #removing any calibrations where the no cal flow went below 5 (when it is set at 10sccm)
  filter(!ch1_sens < 1) %>% #removing any calibrations where sensitiivty is wrong
  filter(!ch2_sens < 1) |> 
  filter(ce < 1) %>% 
  filter(ce > 0) %>% 
  filter(!date == "2025-10-17 12:00:00") %>% #dodgy cal (after instrument switched back on? multi cal day)
  filter(!date == "2021-01-14 09:00:00") %>% #inlet pressure is messed up on this cal
  filter(!date == "2021-01-12 21:00:00")


cal_coefficients |> 
  mutate(ch1_sens_pres_cor = ch1_sens/av_rxn_vessel_pressure, 
         ch2_sens_pres_cor = ch2_sens/av_rxn_vessel_pressure) |> 
  pivot_longer(cols = c(ch1_sens, ch2_sens, ch1_sens_pres_cor, ch2_sens_pres_cor), 
               names_to = "channel", values_to = "sens") |> 
  ggplot(aes(date, sens, color = channel)) +
    geom_line()

get_period <- function(datetime) {
  case_when(
    datetime <= "2020-10-13 01:00:00" ~ "Period 1",
    datetime >= "2020-10-13 02:00:00" & datetime < as.Date("2021-01-11") ~ "Period 2",
    datetime >= as.Date("2021-01-11") & datetime < as.Date("2021-06-11") ~ "Period 3",
    datetime >= as.Date("2021-06-11") & datetime < as.Date("2021-12-31") ~ "Period 4",
    datetime >= as.Date("2021-12-31") & datetime < as.Date("2022-11-01") ~ "Period 5",
    datetime >= as.Date("2022-11-01") & datetime < as.Date("2023-10-19") ~ "Period 6",
    datetime >= as.Date("2023-10-19") & datetime < as.Date("2024-02-08") ~ "Period 7",
    datetime >= as.Date("2024-02-08") & datetime < as.Date("2025-02-03") ~ "Period 8",
    datetime >= as.Date("2025-02-03") & datetime < as.Date("2025-03-18") ~ "Period 9",
    datetime >= as.Date("2025-03-18") & datetime < as.Date("2025-06-05") ~ "Period 10",
    datetime >= as.Date("2025-06-05") & datetime < as.Date("2025-11-13") ~ "Period 11",
    datetime >= as.Date("2025-11-13") & datetime < as.Date("2025-12-30") ~ "Period 12",
    datetime >= as.Date("2025-12-30") & datetime < as.Date("2026-07-16") ~ "Period 13",
    datetime >= as.Date("2026-07-16") & datetime < as.Date("2026-12-31") ~ "Period 14",
    TRUE ~ NA_character_  
  )
}

plot_data = cal_coefficients |> 
  mutate(period = get_period(date)) |> 
  rename("Channel 1 Sensitivity" = ch1_sens, 
         "Channel 2 Sensitivity" = ch2_sens, 
         "Avg reaction vessel pressure" = av_rxn_vessel_pressure) |> 
  pivot_longer(cols = c("Channel 1 Sensitivity", "Channel 2 Sensitivity", "Avg reaction vessel pressure"), 
               names_to = "type", values_to = "counts") |> 
  mutate(type = factor(type, levels = c("Channel 1 Sensitivity", "Channel 2 Sensitivity", "Avg reaction vessel pressure"))) 

period_lines <- plot_data |>
  distinct(date, period) |>
  arrange(date) |>
  mutate(period_change = period != lag(period)) |>
  filter(period_change) |> 
  mutate(
    short_label = gsub("Period ", "", period)
  )



# cylinder changes lines

red_lines <- data.frame(
  date = as_datetime(c("2021-09-01 00:00:00", "2025-02-04 00:00:00", 
                       "2026-08-12 00:00:00"))
)

sens_pressure = ggplot(plot_data, aes(date, counts, color = period)) +
  geom_point() +
  geom_vline(
    data = period_lines,
    aes(xintercept = as.numeric(date)),
    linetype = "dotted",
    colour = "grey40"
  ) +
  geom_vline(
    data = red_lines,
    aes(xintercept = as.numeric(date)),
    linetype = "solid",
    colour = "darkred",
    linewidth = 0.7
  ) +
  geom_text(
    data = period_lines,
    aes(
      x = date,
      y = Inf,
      label = short_label
    ),
    inherit.aes = FALSE,
    angle = 0,
    vjust = 1.5,
    hjust = -0.5,
    colour = "grey40",
    size = 2.3, 
    fontface = "bold"
  ) +
  facet_wrap(~type, ncol = 1, scales = "free_y") +
  theme_bw() +
  theme(
    legend.position = "none",
    
    # Facet labels
    strip.text = element_text(size = 14),
    
    # Axis titles
    axis.title.x = element_text(size = 14),
    axis.title.y = element_text(size = 14),
    
    # Optional: axis tick labels
    axis.text = element_text(size = 12)
  ) +
  labs(x = "Year", y = " ") +
  scale_color_manual(
    values =  paletteer_d("ggthemes::manyeys")
  )


ggsave(path = "plots/thesis_plots", plot = sens_pressure, device = "png", filename = "sens_presssure_timeseries.png", 
       dpi = 300, width = 10, height = 8)

paletteer_d("ggthemes::manyeys")
paletteer_d("colorBlindness::SteppedSequential5Steps")


lm_periods <- c("Period 1", "Period 3", "Period 4", 
                "Period 7", "Period 8", "Period 10", 
                "Period 11", "Period 12", "Period 13")
interp_periods <- c("period2", "period5", "period6", "period9", "Period 14") #the linear relationships are not strong enough here, instead we just linearly interpolate

# now we look at the relationship for each time period

sens_press_relationships = cal_coefficients |> 
  mutate(period = get_period(date)) |> 
  # pivot_longer(cols = c(ch1_sens, ch2_sens), names_to = "channel", 
  #              values_to = "sens") |> 
  mutate(period = factor(period, levels = c("Period 1", "Period 2", "Period 3", "Period 4", 
                                            "Period 5", "Period 6", "Period 7", "Period 8", 
                                            "Period 9", "Period 10", "Period 11", "Period 12", 
                                            "Period 13", "Period 14"))) |> 
  mutate(
    line_type = if_else(
      period %in% lm_periods,
      "Interpolated pressure",
      "Interpolated linear"
    )
  ) |> 
  ggplot(aes(av_rxn_vessel_pressure, ch1_sens, color = period)) +
  geom_point() +
  geom_smooth(
    aes(linetype = line_type),
    method = "lm",
    se = FALSE, color = "grey40"
  ) +
  facet_wrap(~period, scales = "free") +
  theme_bw() +
  theme(
    legend.position = "none",
    
    # Facet labels
    strip.text = element_text(size = 14),
    
    # Axis titles
    axis.title.x = element_text(size = 14),
    axis.title.y = element_text(size = 14),
    
    # Optional: axis tick labels
    axis.text = element_text(size = 12)
  ) +
  scale_linetype_manual(
    values = c(
      "Interpolated pressure" = "solid",
      "Interpolated linear" = "dashed"
    )
  )+
  labs(x = "Avg reaction vessel pressure", y = "Channel 1 sensitivity") +
  scale_color_manual(
    values =  paletteer_d("ggthemes::manyeys")
  )

ggsave(path = "plots/thesis_plots", plot = sens_press_relationships, device = "png", 
       filename = "sens_press_relationships.png", 
       dpi = 300, width = 10, height = 8)



cal_coefficients |> 
  ggplot(aes(date, av_rxn_vessel_pressure)) +
  geom_point() +
  geom_vline(
    data = period_lines,
    aes(xintercept = as.numeric(date)),
    linetype = "dotted",
    colour = "grey40"
  ) 

 ggplotly()  
  

 cal_coefficients |> 
   ggplot(aes(date, ce)) +
   geom_point() +
   geom_vline(
     data = period_lines,
     aes(xintercept = as.numeric(date)),
     linetype = "dotted",
     colour = "grey40"
   ) 

 
 
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
 
 
 
 range_df <- tibble(
   start = do.call(c, lapply(interp_ranges, `[`, 1)),
   end   = do.call(c, lapply(interp_ranges, `[`, 2))
 )
 
 ggplot() +
   geom_rect(data = range_df,
             aes(xmin = start, xmax = end, ymin = -Inf, ymax = Inf),
             fill = "lightblue", alpha = 0.3) +
   geom_point(data = cal_coefficients, aes(date, ce_zero),
              colour = "grey60", size = 0.8) +
   geom_point(data = interpolate_ce, aes(date, ce_interpolated),
              colour = "red", size = 0.8) +
   coord_cartesian(ylim = c(0.3, 0.8)) +
   geom_vline(
     data = period_lines,
     aes(xintercept = as.numeric(date)),
     linetype = "dotted",
     colour = "grey40"
   ) 
 
 
 ggplotly()
 
 
 # looking at the pressure-interpolated sensitivities 
 
 out_dir <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data"
 
 datetime >= as.Date("2024-02-08") & datetime < as.Date("2025-02-03") ~ "Period 8"
 # ---- PLOT SETTINGS ----
 plot_from <- "2024-02-08" #"2026-01-26"
 plot_to   <- "2025-02-03" #"2026-01-27"
 bin_size  <- "1 hour"
 
 sens_binned <- open_dataset(out_dir, format = "parquet") |>
   filter(datetime >= as.POSIXct(plot_from, tz = "UTC"),
          datetime <= as.POSIXct(plot_to, tz = "UTC")) |>
   mutate(time_bin = floor_date(datetime, bin_size)) |>
   group_by(time_bin, period) |>
   summarise(
     ch1_final  = mean(ch1_sens, na.rm = TRUE),
     ch2_final  = mean(ch2_sens, na.rm = TRUE),
     ch1_model  = mean(ch1_sens_raw, na.rm = TRUE),
     ch2_model  = mean(ch2_sens_raw, na.rm = TRUE),
     ch1_interp = mean(ch1_sens_coeff, na.rm = TRUE),
     ch2_interp = mean(ch2_sens_coeff, na.rm = TRUE),
     frac_model = mean(as.numeric(!is.na(ch1_sens_raw)), na.rm = TRUE),
     pressure   = mean(av_rxn_vessel_pressure, na.rm = TRUE)
   ) |>
   collect() |>
   mutate(method = if_else(frac_model > 0.5, "Pressure model", "Time interpolation"))
 
 
 sens_long <- sens_binned |>
   pivot_longer(c(ch1_final, ch2_final), names_to = "channel", values_to = "sens") |>
   mutate(channel = str_remove(channel, "_final"))
 
 cal_long <- cal_coefficients |>
   mutate(date = as_datetime(date, tz = "UTC")) |>
   filter(date >= as.POSIXct(plot_from, tz = "UTC"),
          date <= as.POSIXct(plot_to, tz = "UTC")) |>
   pivot_longer(c(ch1_sens, ch2_sens), names_to = "channel", values_to = "sens") |>
   mutate(channel = str_remove(channel, "_sens"))
 
 ggplot(sens_long, aes(time_bin, sens)) +
   geom_point(aes(colour = method), size = 0.4, alpha = 0.6) +
   geom_point(data = cal_long, aes(date, sens),
              shape = 21, fill = "white", colour = "black", size = 2) +
   facet_wrap(~channel, ncol = 1, scales = "free_y") +
   labs(x = NULL, y = "Sensitivity", colour = NULL,
        caption = "White points = 36 h calibrations") +
   theme_bw() +
   theme(legend.position = "top") +
   guides(colour = guide_legend(override.aes = list(size = 3, alpha = 1)))

 
 sens_long <- sens_binned |>
   filter(pressure < 200) |> 
   pivot_longer(c(ch1_final, ch2_final, pressure),
                names_to = "panel", values_to = "value") |>
   mutate(panel = recode(panel,
                         ch1_final = "Ch1 sensitivity",
                         ch2_final = "Ch2 sensitivity",
                         pressure  = "Reaction vessel pressure"),
          panel = factor(panel, levels = c("Ch1 sensitivity",
                                           "Ch2 sensitivity",
                                           "Reaction vessel pressure")))
 
 cal_long <- cal_coefficients |>
   mutate(date = as_datetime(date, tz = "UTC")) |>
   filter(date >= as.POSIXct(plot_from, tz = "UTC"),
          date <= as.POSIXct(plot_to, tz = "UTC")) |>
   filter(av_rxn_vessel_pressure < 200) |> 
   pivot_longer(c(ch1_sens, ch2_sens, av_rxn_vessel_pressure),
                names_to = "panel", values_to = "value") |>
   mutate(panel = recode(panel,
                         ch1_sens = "Ch1 sensitivity",
                         ch2_sens = "Ch2 sensitivity",
                         av_rxn_vessel_pressure = "Reaction vessel pressure"),
          panel = factor(panel, levels = levels(sens_long$panel)))
 
 ggplot(sens_long, aes(time_bin, value)) +
   geom_point(aes(colour = method), size = 0.4, alpha = 0.6) +
   geom_point(data = cal_long, aes(date, value),
              shape = 21, fill = "white", colour = "black", size = 2) +
   facet_wrap(~panel, ncol = 1, scales = "free_y", strip.position = "left") +
   labs(x = NULL, y = NULL, colour = NULL,
        caption = "White points = 36 h calibrations") +
   theme_bw() +
   theme(legend.position = "top",
         strip.placement = "outside",
         strip.background = element_blank()) +
   guides(colour = guide_legend(override.aes = list(size = 3, alpha = 1))) 
 
 
 
 
 
 
cal = cal_coefficients |>
   mutate(period = get_period(date),
          pressure = av_rxn_vessel_pressure) |>
   arrange(date)



cal_long <- cal |>
  select(date, period, pressure, Ch1 = ch1_sens, Ch2 = ch2_sens) |>
  pivot_longer(c(Ch1, Ch2), names_to = "channel", values_to = "sens")

count(cal, period)  


cal_long |>
  filter(channel == "Ch1") |>
  select(date, period, `Ch1 sensitivity` = sens, `Reaction vessel pressure` = pressure) |>
  pivot_longer(-c(date, period)) |>
  ggplot(aes(date, value, colour = period)) +
  geom_vline(xintercept = period_starts, linetype = "dotted", colour = "grey50") +
  geom_point(size = 1) +
  facet_wrap(~name, ncol = 1, scales = "free_y") +
  labs(x = NULL, y = NULL, colour = "Period")




cal_long |>
  filter(channel == "Ch1") |>
  ggplot(aes(pressure, sens, colour = date)) +
  geom_point(size = 1.2) +
  geom_smooth(method = "lm", se = FALSE, colour = "black", linewidth = 0.5) +
  facet_wrap(~period, scales = "free", labeller = label_both) +
  scale_colour_datetime(low = "navy", high = "orange") +
  labs(x = "Reaction vessel pressure", y = "Ch1 sensitivity", colour = NULL)


fit_stats <- cal_long |>
  mutate(t = as.numeric(date)) |>
  group_by(channel, period) |>
  filter(n() >= 10) |>
  summarise(
    n               = n(),
    r_pressure_time = cor(pressure, t),
    r2_time         = summary(lm(sens ~ t))$r.squared,
    r2_pressure     = summary(lm(sens ~ pressure))$r.squared,
    pct_per_unit    = 100 * (exp(coef(lm(log(sens) ~ pressure))[[2]]) - 1),
    .groups = "drop"
  ) |> 
  mutate(period = factor(period, levels = c("Period 1", "Period 2", "Period 3", "Period 4", 
                                            "Period 5", "Period 6", "Period 7", "Period 8", 
                                            "Period 9", "Period 10", "Period 11", "Period 12", 
                                            "Period 13", "Period 14"))) |> 
  arrange(period)

write_csv(fit_stats, "data/data/stats_pressure_sens.csv")


fit_stats |> filter(channel == "Ch2") |> print(n = Inf)

fit_stats |>
  pivot_longer(c(r2_time, r2_pressure), names_to = "explained_by", values_to = "r2") |>
  mutate(explained_by = recode(explained_by, r2_time = "Time", r2_pressure = "Pressure")) |>
  ggplot(aes(period, r2, fill = explained_by)) +
  geom_col(position = "dodge") +
  facet_wrap(~channel, ncol = 1) +
  labs(x = "Period", y = expression(R^2), fill = "Explained by")


cal_dev <- cal_long |>
  group_by(channel, period) |>
  mutate(
    p_dev        = pressure  - rollapply(pressure,  9, median, partial = TRUE),
    log_sens_dev = log(sens) - rollapply(log(sens), 9, median, partial = TRUE)
  ) |>
  ungroup()

# One pooled estimate per channel (log scale -> a % effect)
beta <- cal_dev |>
  group_by(channel) |>
  summarise(beta = coef(lm(log_sens_dev ~ p_dev))[["p_dev"]]) |>
  mutate(pct_per_unit = 100 * (exp(beta) - 1))
beta

# Pooled scatter. Look for: all periods falling on the same curve.
curve_df <- crossing(beta, p_dev = seq(-1.5, 4, by = 0.1)) |>
  mutate(pct = 100 * (exp(beta * p_dev) - 1))

p_short <- cal_dev |>
  ggplot(aes(p_dev, 100 * (exp(log_sens_dev) - 1))) +
  geom_hline(yintercept = 0, colour = "grey60") +
  geom_vline(xintercept = 0, colour = "grey60") +
  geom_point(aes(colour = period), size = 1, alpha = 0.7) +
  geom_line(data = curve_df, aes(p_dev, pct), linewidth = 1) +
  facet_wrap(~channel) +
  coord_cartesian(xlim = c(-3, 5), ylim = c(-40, 25)) +
  labs(x = "Pressure minus local median",
       y = "Sensitivity vs local median (%)", colour = "Period")



slope_ci <- function(x, y) {
  fit <- lm(y ~ x)
  ci  <- confint(fit)["x", ]
  tibble(est = coef(fit)[["x"]], lo = ci[[1]], hi = ci[[2]])
}

per_period <- cal_dev |>
  group_by(channel, period) |>
  filter(n() >= 8, sd(p_dev) > 0.05) |>
  summarise(slope_ci(p_dev, log_sens_dev), sd_p = sd(p_dev), .groups = "drop") |>
  mutate(across(c(est, lo, hi), ~ 100 * (exp(.x) - 1)))

per_period |>
  ggplot(aes(period, est, ymin = lo, ymax = hi, colour = sd_p)) +
  geom_hline(data = beta, aes(yintercept = pct_per_unit), linetype = "dashed") +
  geom_pointrange() +
  facet_wrap(~channel, ncol = 1) +
  coord_cartesian(ylim = c(-40, 30)) +
  scale_colour_viridis_c(name = "Pressure\nvariation") +
  labs(x = "Period", y = "% change in sensitivity per unit pressure")


long_term <- cal_long |>
  filter(period %in% 4:7) |>
  left_join(beta, by = "channel") |>
  group_by(channel) |>
  mutate(
    s_start = median(head(sens, 10)),
    p_start = median(head(pressure, 10)),
    `If short-term effect applied` = s_start * exp(beta * (pressure - p_start))
  ) |>
  ungroup() |>
  select(date, channel, Actual = sens, `If short-term effect applied`) |>
  pivot_longer(-c(date, channel))

long_term |>
  ggplot(aes(date, value, colour = name)) +
  geom_point(size = 1) +
  facet_wrap(~channel, ncol = 1, scales = "free_y") +
  labs(x = NULL, y = "Sensitivity", colour = NULL) +
  theme(legend.position = "top")



cal_norm <- cal_long |>
  left_join(select(beta, channel, beta), by = "channel") |>
  mutate(sens_norm = sens * exp(-beta * (pressure - p_ref)))

cal_norm |>
  filter(period %in% 11:13) |>
  select(date, channel, `As measured` = sens, `Normalised to p_ref` = sens_norm) |>
  pivot_longer(-c(date, channel)) |>
  ggplot(aes(date, value, colour = name)) +
  geom_vline(xintercept = period_starts, linetype = "dotted", colour = "grey50") +
  geom_point(size = 1) +
  facet_wrap(~channel, ncol = 1, scales = "free_y") +
  coord_cartesian(xlim = as.POSIXct(c("2025-06-01", "2026-05-01"), tz = "UTC")) +
  labs(x = NULL, y = "Sensitivity", colour = NULL) +
  theme(legend.position = "top")


# A number for "smoothness": typical % jump between consecutive calibrations
jumpiness <- cal_norm |>
  group_by(channel, period) |>
  summarise(
    raw        = median(abs(diff(log(sens))), na.rm = TRUE) * 100,
    normalised = median(abs(diff(log(sens_norm))), na.rm = TRUE) * 100,
    .groups = "drop"
  )
jumpiness |> filter(channel == "Ch1") |> print(n = Inf)


loo_one_period <- function(d, beta, n_smooth = 5) {
  n <- nrow(d)
  if (n < 8) return(tibble())
  t    <- as.numeric(d$date)
  s    <- d$sens
  p    <- d$pressure
  norm <- s * exp(-beta * (p - p_ref))
  
  map_dfr(2:(n - 1), function(i) {
    k   <- -i
    fit <- lm(s[k] ~ p[k])
    norm_s <- rollapply(norm[k], n_smooth, median, partial = TRUE)
    tibble(
      date     = d$date[i],
      observed = s[i],
      time            = approx(t[k], s[k], xout = t[i])$y,
      lm              = coef(fit)[[1]] + coef(fit)[[2]] * p[i],
      anchored        = approx(t[k], norm[k], xout = t[i])$y * exp(beta * (p[i] - p_ref)),
      anchored_smooth = approx(t[k], norm_s,  xout = t[i])$y * exp(beta * (p[i] - p_ref))
    )
  })
}

loo <- cal_norm |>
  group_by(channel, period) |>
  group_modify(~ loo_one_period(.x, beta = .x$beta[1])) |>
  ungroup()

loo_err <- loo |>
  pivot_longer(c(time, lm, anchored, anchored_smooth),
               names_to = "method", values_to = "predicted") |>
  mutate(method = factor(method, levels = c("time", "lm", "anchored", "anchored_smooth")),
         abs_pct_err = 100 * abs(predicted / observed - 1))

# Overall
loo_err |>
  group_by(channel, method) |>
  summarise(mean_err = mean(abs_pct_err), median_err = median(abs_pct_err), .groups = "drop") |>
  print(n = Inf)

# By period
loo_err |>
  group_by(channel, period, method) |>
  summarise(mean_err = mean(abs_pct_err), .groups = "drop") |>
  ggplot(aes(period, mean_err, fill = method)) +
  geom_col(position = "dodge") +
  facet_wrap(~channel, ncol = 1) +
  coord_cartesian(ylim = c(0, 20)) +
  labs(x = "Period", y = "Mean absolute error predicting a held-out calibration (%)",
       fill = "Method")


# What the predictions look like for one period
example_period <- 13
loo |>
  filter(period == example_period) |>
  pivot_longer(c(time, lm, anchored_smooth), names_to = "method", values_to = "predicted") |>
  ggplot(aes(date)) +
  geom_line(aes(y = predicted, colour = method)) +
  geom_point(aes(y = observed), shape = 21, fill = "white", size = 2) +
  facet_wrap(~channel, ncol = 1, scales = "free_y") +
  labs(x = NULL, y = "Sensitivity", colour = "Prediction",
       title = paste("Period", example_period, "- white points are the held-out calibrations")) +
  theme(legend.position = "top")




# 2. Estimate the short-term pressure effect (each cal vs its ~9 neighbours)
beta <- cal_long |>
  group_by(channel, period) |>
  mutate(p_dev   = pressure  - rollapply(pressure,  9, median, partial = TRUE),
         log_dev = log(sens) - rollapply(log(sens), 9, median, partial = TRUE)) |>
  group_by(channel) |>
  summarise(beta = coef(lm(log_dev ~ p_dev))[[2]])

beta |> mutate(pct_per_unit = 100 * (exp(beta) - 1))   # should be about -10 %

# 3. Remove only the short-term pressure effect
cal_local <- cal_long |>
  left_join(beta, by = "channel") |>
  group_by(channel, period) |>
  mutate(p_local    = rollapply(pressure, 9, median, partial = TRUE),
         sens_local = sens * exp(-beta * (pressure - p_local))) |>
  ungroup()

# 4. Plot: raw calibrations in grey, pressure-corrected on top
cal_local |>
  ggplot(aes(date)) +
  geom_vline(xintercept = period_starts, linetype = "dotted", colour = "grey50") +
  geom_point(aes(y = sens), colour = "grey75", size = 0.8) +
  geom_point(aes(y = sens_local, colour = period), size = 1) +
  facet_wrap(~channel, ncol = 1, scales = "free_y") +
  labs(x = NULL, y = "Sensitivity (short-term pressure effect removed)",
       colour = "Period") +
  theme_bw()

cal_pairs <- cal_long |>
  group_by(channel, period) |>
  arrange(date, .by_group = TRUE) |>
  mutate(
    days  = as.numeric(difftime(date, lag(date), units = "days")),  # time since previous cal
    dp    = pressure - lag(pressure),                               # pressure change
    ratio = sens / lag(sens)                                        # sensitivity change (ratio)
  ) |>
  ungroup() |>
  filter(days <= 2.5, abs(dp) >= 1.5) |>        # close together, big pressure change
  mutate(pct_per_unit = 100 * (ratio^(1 / dp) - 1))

# Overall, per channel (should be about -10 %)
cal_pairs |>
  group_by(channel) |>
  summarise(n = n(), median_pct_per_unit = median(pct_per_unit))

# By year, to check whether it changes over time
cal_pairs |>
  mutate(year = year(date)) |>
  group_by(channel, year) |>
  summarise(n = n(), median_pct_per_unit = median(pct_per_unit), .groups = "drop")

cal_pairs |>
  mutate(year = year(date)) |>
  group_by(year) |>
  summarise(n = n(), median_pct_per_unit = median(pct_per_unit))


cal_pairs |>
  mutate(year = year(date)) |>
  group_by(year) |>
  summarise(n = n(),
            median = median(pct_per_unit),
            q25 = quantile(pct_per_unit, 0.25),
            q75 = quantile(pct_per_unit, 0.75))



p_ref <- 12

period_starts <- as.POSIXct(c(
  "2020-10-13 02:00:00",  # 2
  "2021-01-11",           # 3
  "2021-06-11",           # 4
  "2021-12-31",           # 5
  "2022-11-01",           # 6  pump maintenance
  "2023-10-19",           # 7  unexplained pressure change
  "2024-02-08",           # 8  pump replaced
  "2025-02-03",           # 9  instrument back after maintenance
  "2025-03-18",           # 10 pump moved in front of rack
  "2025-06-05",           # 11
  "2025-11-13",           # 12 pump replaced
  "2025-12-30",           # 13
  "2026-07-16",           # 14
  "2026-08-12 08:00:00"   # 15 step change in calibrations - cause to confirm
), tz = "UTC")

cal_corr <- cal_long |>
  left_join(beta, by = "channel") |>
  mutate(sens_corr = sens * exp(-beta * (pressure - p_ref)))

sens_panel <- cal_corr |>
  ggplot(aes(date)) +
  geom_vline(xintercept = period_starts, linetype = "dotted", colour = "grey50") +
  geom_point(aes(y = sens, colour = "Measured"), size = 0.8) +
  geom_point(aes(y = sens_corr, colour = paste("Corrected to pressure", p_ref)), size = 0.8) +
  facet_wrap(~channel, ncol = 1, scales = "free_y") +
  scale_colour_manual(values = c("grey60", "firebrick")) +
  labs(x = NULL, y = "Sensitivity", colour = NULL) +
  theme_bw() + theme(legend.position = "top")

pressure_panel <- cal_corr |>
  filter(channel == "Ch1") |>
  ggplot(aes(date, pressure)) +
  geom_vline(xintercept = period_starts, linetype = "dotted", colour = "grey50") +
  geom_point(size = 0.8) +
  labs(x = NULL, y = "Reaction vessel pressure") +
  theme_bw()

sens_panel
pressure_panel




effect_per_unit <- 0.89   # -11 % sensitivity per unit pressure
n_edge <- 5               # calibrations used at the start and end of each period

period_table <- cal_long |>
  arrange(date) |>
  group_by(channel, period) |>
  filter(n() >= 2 * n_edge) |>              # need enough calibrations for start and end
  summarise(
    start      = min(date),
    end        = max(date),
    n          = n(),
    p_start    = median(head(pressure, n_edge)),
    p_end      = median(tail(pressure, n_edge)),
    sens_start = median(head(sens, n_edge)),
    sens_end   = median(tail(sens, n_edge)),
    .groups = "drop"
  ) |>
  mutate(
    pressure_change         = p_end - p_start,
    predicted_from_pressure = sens_start * effect_per_unit^pressure_change,
    pct_from_pressure       = 100 * (predicted_from_pressure / sens_start - 1),
    pct_measured            = 100 * (sens_end / sens_start - 1)
  ) |>
  mutate(across(where(is.numeric), ~ round(.x, 2)))

pres_correct_sens = period_table |>
  filter(channel == "Ch1") |>
  select(period, n, pressure_change, sens_start, predicted_from_pressure, sens_end,
         pct_from_pressure, pct_measured) |> 
  mutate(period = factor(period, levels = c("Period 1", "Period 2", "Period 3",
                                            "Period 4", "Period 5", "Period 6", 
                                            "Period 7", "Period 8", "Period 9", 
                                            "Period 10", "Period 11", "Period 12", 
                                            "Period 13", "Period 14"))) |> 
  arrange(period)

write_csv(pres_correct_sens, "data/pres_correct_sens.csv")
