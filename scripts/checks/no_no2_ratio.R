


library(tidyverse)
library(plotly)


files <- list.files("/mnt/longship/projects/chem-bttao-2026/btt_processing/processing/calibration_data",
                    pattern = "ce_params.csv", recursive = TRUE, full.names = TRUE)

cal_stages <- map_dfr(files, read_csv, show_col_types = FALSE)

unique(cal_stages$type)   # check the labels -- adjust the regex/case_when below if needed

stages <- cal_stages |>
  mutate(
    gpt = str_extract(type, "(?<=gpt_)\\d"),
    blc = str_extract(type, "(?<=blc_)\\d"),
    stage = case_when(
      gpt == "1" & blc == "0" ~ "s1",
      gpt == "1" & blc == "1" ~ "s2",
      gpt == "0" & blc == "0" ~ "s3",
      gpt == "0" & blc == "1" ~ "s4"
    ),
    # zero-corrected, flow-normalised NOx-channel signal (Hz per sccm)
    sig    = (ch2_hz_med - zero_ch2) / no_cal_flow_mean,
    sig_sd = ch2_hz_sd / no_cal_flow_mean
  )

# one CE per calibration (take whichever row carries it)
ce_by_date <- stages |>
  group_by(date) |>
  summarise(ce = first(na.omit(ce)), .groups = "drop")

ratio <- stages |>
  filter(stage %in% c("s3", "s4")) |>
  select(date, stage, sig, sig_sd) |>
  pivot_wider(names_from = stage, values_from = c(sig, sig_sd)) |>
  left_join(ce_by_date, by = "date") |>
  mutate(
    no2_no = (sig_s4 - sig_s3) / (ce * sig_s3),
    # error propagation from the stage SDs (CE uncertainty ignored)
    no2_no_err = sqrt((sig_sd_s4 / (ce * sig_s3))^2 +
                        (sig_s4 * sig_sd_s3 / (ce * sig_s3^2))^2)
  )

red_lines <- data.frame(
  date = as_datetime(c("2021-09-01 00:00:00", "2025-02-04 00:00:00", 
                       "2026-08-12 00:00:00"))
)

ratio |> 
  filter(no2_no < 1) |> 
  ggplot(aes(date, no2_no * 100)) +
  geom_hline(yintercept = 0, linetype = 2, colour = "grey50") +
  geom_vline(data = red_lines, aes(xintercept = date), colour = "red") +
  # geom_errorbar(aes(ymin = (no2_no - no2_no_err) * 100,
  #                   ymax = (no2_no + no2_no_err) * 100),
  #               width = 0, colour = "grey60") +
  geom_point() +
  labs(x = NULL, y = expression(NO[2]/NO~"in cylinder (%)")) +
  theme_bw()

ggplotly()


# ==== The cylinder removed on 12 Aug 2026: straight line, where it crosses 0 ====

cal_dir   <- "/mnt/longship/projects/chem-bttao-2026/btt_processing/processing/calibration_data"
cyl_on    <- ymd_hms("2025-02-03 00:00:00", tz = "UTC")   # went on (maintenance + cylinder change)
cyl_off   <- ymd_hms("2026-08-12 08:00:00", tz = "UTC")   # came off (measured 30% low)
cyl_error <- 0.05                                          # certificate uncertainty

# ---- 1. NO2/NO (x) for each calibration while this cylinder was on ----
x_data <- list.files(cal_dir, pattern = "ce_params.csv", recursive = TRUE, full.names = TRUE) %>%
  map_df(read_csv, show_col_types = FALSE) %>%
  mutate(date = as_datetime(date, tz = "UTC"),
         sig  = (ch2_hz_med - zero_ch2) / no_cal_flow_mean) %>%
  filter(date >= cyl_on, date < cyl_off) %>%
  group_by(date) %>%
  summarise(s3 = sig[type == "gpt_0_blc_0"][1],          # stage 3: NO only
            s4 = sig[type == "gpt_0_blc_1"][1],          # stage 4: NO + converted NO2
            ce = first(na.omit(ce)), .groups = "drop") %>%
  mutate(x    = (s4 - s3) / (ce * s3),
         days = as.numeric(difftime(date, cyl_on, units = "days"))) %>%
  filter(between(x, -0.2, 1))                            # drop broken calibrations

# ---- 2. straight line: x = a + b * days since it went on ----
fit <- lm(x ~ days, data = x_data)
a   <- coef(fit)[["(Intercept)"]]          # x on the day it went on
b   <- coef(fit)[["days"]]                 # change in x per day
ci  <- confint(fit)["(Intercept)", ]
zero_date <- cyl_on + as.difftime(-a / b, units = "days")
x_off     <- a + b * as.numeric(difftime(cyl_off, cyl_on, units = "days"))

message(sprintf("Calibrations used:        %d", nrow(x_data)))
message(sprintf("x when it went on:        %.1f%% (95%% range %.1f to %.1f)", 100 * a, 100 * ci[1], 100 * ci[2]))
message(sprintf("Within the 5%% error?      %s", if (abs(a) <= cyl_error) "YES - treat as clean when new" else "NO"))
message(sprintf("Degradation rate:         %.1f%% per year", 100 * b * 365.25))
message(sprintf("Line reaches 0 on:        %s", as_date(zero_date)))
message(sprintf("x when it came off:       %.0f%%  -> NO reads %.0f%% low (measured: 30%% low)",
                100 * x_off, 100 * (1 - 1 / (1 + x_off))))
message(sprintf("R squared:                %.2f", summary(fit)$r.squared))

# ---- 3. plot ----
line_from <- min(zero_date, cyl_on)
fit_line  <- tibble(date = seq(line_from, cyl_off, length.out = 200)) %>%
  mutate(x = a + b * as.numeric(difftime(date, cyl_on, units = "days")))

ggplot(x_data, aes(date, 100 * x)) +
  annotate("rect", xmin = line_from, xmax = cyl_off, ymin = -100 * cyl_error, ymax = 100 * cyl_error,
           fill = "grey85", alpha = 0.6) +                              # +/- 5% certificate error
  geom_hline(yintercept = 0, colour = "grey40") +
  geom_vline(xintercept = c(cyl_on, cyl_off), colour = "red", linetype = "dashed") +
  geom_point(colour = "#0072B2", size = 1.2, alpha = 0.6) +
  geom_smooth(method = "loess", se = FALSE, colour = "grey30", linewidth = 0.5, linetype = "dotted") +
  geom_line(data = fit_line, aes(date, 100 * x), colour = "black", linewidth = 0.8) +
  annotate("point", x = zero_date, y = 0, shape = 4, size = 4, stroke = 1.5) +
  labs(x = NULL, y = "NO2 / NO in cylinder (%)",
       title = sprintf("Cylinder removed 12 Aug 2026: %.1f%% when it went on, reaches 0 on %s",
                       100 * a, as_date(zero_date)),
       subtitle = "Black = straight line; dotted = loess (shape check); grey band = +/-5% certificate error; red = on / off") +
  theme_bw()
