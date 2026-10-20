# =====================================================================
# cylinder_correction.R
#
# Degradation of the NO calibration cylinder in use 2025-02-03 to 2026-08-12.
#
#   1. Lab calibration of the returned cylinder  -> how much NO it actually held at the end
#   2. NO2/NO in the cylinder from every field calibration (CE stages 3 and 4)
#   3. Straight line through NO2/NO against time
#   4. Offset: the line at the lab date vs the lab result
#   5. Correction used in process_calibrations: x(t) = cyl_a + cyl_b * years since installation
#   6. Sensitivity test: same correction built from a loess instead of a straight line
#   7. Figures for the write-up
#
# Assumes the NO lost from the cylinder was oxidised to NO2 (NO + NO2 conserved), so
#   NO in cylinder = certificate / (1 + x),   x = NO2/NO
# and every sensitivity made with this cylinder is multiplied by (1 + x).
# =====================================================================

library(tidyverse)

# ==== SETTINGS ====
cal_dir   <- "/mnt/longship/projects/chem-bttao-2026/btt_processing/processing/calibration_data"
cyl_on    <- ymd_hms("2025-02-03 00:00:00", tz = "UTC")   # cylinder installed
cyl_off   <- ymd_hms("2026-08-12 08:00:00", tz = "UTC")   # cylinder removed
lab_date  <- ymd_hms("2026-08-26 08:00:00", tz = "UTC")   # when the lab calibration was done 

cyl_cert  <- 5.1     # certified NO in the cylinder (ppm)
cyl_error <- 0.05    # certificate uncertainty (fraction)
lab_assumed_conc <- 5.1   # cylinder concentration the lab used to work out the "set" concentrations (ppm)

# lab calibration of the returned cylinder (diluted to set concentrations, measured against NPL 1 ppm)
lab <- tibble(set      = c(50, 70, 100, 200),                          # ppb
              measured = c(32.72505, 46.08710, 68.09394, 132.69637),   # ppb
              sd       = c(0.0341079, 0.0265548, 0.0145165, 0.2673654))

loess_span <- 0.75    # smoothness of the loess used in the sensitivity test
save_figs  <- FALSE   # TRUE writes the figures below as png files
fig_dir    <- "."


# ==== 1. LAB CALIBRATION: HOW MUCH NO WAS LEFT ====
# measured = fraction * set. Forced through zero: no gas in, no signal out.
lab_fit      <- lm(measured ~ 0 + set, data = lab)
lab_frac     <- coef(lab_fit)[["set"]]                         # fraction of the expected NO actually there
lab_frac_se  <- summary(lab_fit)$coefficients["set", "Std. Error"]

no_end       <- lab_frac * lab_assumed_conc                    # NO actually in the cylinder (ppm)
deficit_end  <- 1 - no_end / cyl_cert                          # fraction below the certificate
x_lab        <- cyl_cert / no_end - 1                          # NO2/NO implied (NO + NO2 = certificate)
x_lab_range  <- cyl_cert * (1 + c(-1, 1) * cyl_error) / no_end - 1   # range from the 5% certificate uncertainty

# ==== 2. NO2/NO FROM EVERY FIELD CALIBRATION WITH THIS CYLINDER ====
# stage 3 (GPT off, BLC off): NO only;  stage 4 (GPT off, BLC on): NO + CE * NO2
x_data <- list.files(cal_dir, pattern = "ce_params.csv", recursive = TRUE, full.names = TRUE) %>%
  map_df(read_csv, show_col_types = FALSE) %>%
  mutate(date = as_datetime(date, tz = "UTC"),
         sig  = (ch2_hz_med - zero_ch2) / no_cal_flow_mean) %>%       # zero-corrected, per sccm of cal gas
  filter(date >= cyl_on, date < cyl_off) %>%
  group_by(date) %>%
  summarise(s3 = sig[type == "gpt_0_blc_0"][1],
            s4 = sig[type == "gpt_0_blc_1"][1],
            ce = first(na.omit(ce)), .groups = "drop") %>%
  mutate(x     = (s4 - s3) / (ce * s3),                                # NO2/NO in the cylinder
         years = as.numeric(difftime(date, cyl_on, units = "days")) / 365.25) %>%
  filter(between(x, -0.2, 1))                                          # broken calibrations

# outliers: more than 3 MAD from a first straight line (e.g. instrument settling after maintenance)
fit0   <- lm(x ~ years, data = x_data)
x_data <- x_data %>% mutate(outlier = abs(residuals(fit0) - median(residuals(fit0))) > 3 * mad(residuals(fit0)))
used   <- filter(x_data, !outlier)


# ==== 3. STRAIGHT LINE: x = a + b * years since installation ====
lin   <- lm(x ~ years, data = used)
a     <- coef(lin)[["(Intercept)"]]                     # NO2/NO at installation, as measured
b     <- coef(lin)[["years"]]                           # increase in NO2/NO per year
a_ci  <- confint(lin)["(Intercept)", ]
b_ci  <- confint(lin)["years", ]
zero_date <- cyl_on + as.difftime(-a / b * 365.25, units = "days")   # when the line reaches 0

yrs_lab <- as.numeric(difftime(lab_date, cyl_on, units = "days")) / 365.25
yrs_off <- as.numeric(difftime(cyl_off,  cyl_on, units = "days")) / 365.25
x_fit_lab <- a + b * yrs_lab                            # what the field calibrations say at the lab date


# ==== 4. OFFSET: FIELD CALIBRATIONS vs LAB ====
# The lab result sets the absolute level; the field calibrations give the change through time.
# The offset is so minimal that I am not going to apply one 

offset <- 0                       


# ==== 5. CORRECTION FOR process_calibrations ====
cyl_a <- a - offset                                     # NO2/NO at installation, anchored to the lab
cyl_b <- b                                              # increase per year

x_corr <- function(years) cyl_a + cyl_b * years          # NO2/NO in the cylinder
f_corr <- function(years) 1 + x_corr(years)              # sensitivity correction factor


# ==== 6. SENSITIVITY TEST: LOESS INSTEAD OF A STRAIGHT LINE ====
# Same steps, no assumed shape: loess through the same points, anchored to the lab the same way.
lo      <- loess(x ~ years, data = used, span = loess_span)
pred_lo <- function(years) predict(lo, newdata = tibble(years = pmin(pmax(years, min(used$years)), max(used$years))))
lo_offset <- 0 #pred_lo(yrs_lab) - x_lab
f_loess <- function(years) 1 + pred_lo(years) - lo_offset

cmp <- tibble(years = x_data$years) %>%                 # at every field calibration with this cylinder
  mutate(f_linear = f_corr(years), f_loess = f_loess(years),
         diff_pct = 100 * (f_loess / f_linear - 1))


# ==== SUMMARY: NUMBERS FOR WRITE-UP ====
cat("\n================ CYLINDER CORRECTION ================\n")
cat(sprintf("Lab calibration of returned cylinder:\n"))
cat(sprintf("  measured / set              %.3f +/- %.3f\n", lab_frac, lab_frac_se))
cat(sprintf("  NO left in cylinder         %.2f ppm (certificate %.2f ppm)\n", no_end, cyl_cert))
cat(sprintf("  NO below certificate        %.1f%%\n", 100 * deficit_end))
cat(sprintf("  implied NO2/NO              %.1f%% (%.1f to %.1f%% with the 5%% certificate uncertainty)\n",
            100 * x_lab, 100 * x_lab_range[1], 100 * x_lab_range[2]))
cat(sprintf("\nField calibrations (CE stages):\n"))
cat(sprintf("  calibrations                %d (%d outliers excluded)\n", nrow(x_data), sum(x_data$outlier)))
cat(sprintf("  NO2/NO at installation (a)  %.1f%% (95%% CI %.1f to %.1f)\n", 100 * a, 100 * a_ci[1], 100 * a_ci[2]))
cat(sprintf("  increase per year (b)       %.1f%% (95%% CI %.1f to %.1f)\n", 100 * b, 100 * b_ci[1], 100 * b_ci[2]))
cat(sprintf("  R squared                   %.2f\n", summary(lin)$r.squared))
cat(sprintf("  line reaches 0 on           %s\n", as_date(zero_date)))
cat(sprintf("  NO2/NO at lab date          %.1f%%\n", 100 * x_fit_lab))
cat(sprintf("\nOffset (field - lab)          %+.1f%% NO2/NO\n", 100 * offset))
cat(sprintf("\nCorrection: x = cyl_a + cyl_b * years since %s\n", as_date(cyl_on)))
cat(sprintf("  cyl_a = %.4f\n  cyl_b = %.4f\n", cyl_a, cyl_b))
cat(sprintf("  factor at installation      %.3f\n", f_corr(0)))
cat(sprintf("  factor at removal           %.3f (lab: %.3f)\n", f_corr(yrs_off), cyl_cert / no_end))
cat(sprintf("\nSensitivity test (loess vs linear):\n"))
cat(sprintf("  mean |difference|           %.1f%%\n", mean(abs(cmp$diff_pct))))
cat(sprintf("  max |difference|            %.1f%%\n", max(abs(cmp$diff_pct))))
cat("=====================================================\n\n")


# ==== 7. FIGURES ====
fit_cols <- c("Linear" = "black", "Loess" = "#D55E00")

# Fig 1. Lab calibration of the returned cylinder
fig1 <- ggplot(lab, aes(set, measured)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", colour = "grey50") +   # what a good cylinder gives
  geom_abline(slope = lab_frac, intercept = 0, colour = "#0072B2", linewidth = 0.8) +
  geom_point(size = 2.5) +
  annotate("text", x = min(lab$set), y = max(lab$set) * 0.95, hjust = 0,
           label = sprintf("measured = %.3f x set\n1:1 (dashed) = cylinder at certificate", lab_frac)) +
  coord_equal(xlim = c(0, max(lab$set) * 1.05), ylim = c(0, max(lab$set) * 1.05)) +
  labs(x = "Set NO (ppb)", y = "Measured NO (ppb)") +
  theme_bw()
print(fig1)

# Fig 2. NO2/NO in the cylinder through the deployment
curve <- tibble(date = seq(cyl_on, cyl_off, length.out = 300)) %>%
  mutate(years = as.numeric(difftime(date, cyl_on, units = "days")) / 365.25,
         Linear = 100 * (a + b * years),                # as fitted to the field calibrations
         Loess  = 100 * pred_lo(years)) %>%
  pivot_longer(c(Linear, Loess), names_to = "fit", values_to = "x")

corrected <- tibble(date = seq(cyl_on, cyl_off, length.out = 300)) %>%
  mutate(x = 100 * x_corr(as.numeric(difftime(date, cyl_on, units = "days")) / 365.25))

fig2 <- ggplot(x_data, aes(date, 100 * x)) +
  geom_point(aes(shape = outlier), colour = "#4A5483", alpha = 0.6, size = 1.2) +
  scale_shape_manual(values = c(`FALSE` = 16, `TRUE` = 4), labels = c("Field calibration", "Outlier"), name = NULL) +
  geom_line(data = curve, aes(date, x, colour = fit), linewidth = 0.8) +
  geom_line(data = curve, aes(date, x, colour = fit), linewidth = 0.8) +
  annotate("pointrange", x = lab_date, y = 100 * x_lab,
           ymin = 100 * x_lab_range[1], ymax = 100 * x_lab_range[2], colour = "black", shape = 17) +
  scale_colour_manual(values = fit_cols, name = NULL) +
  labs(x = NULL, y = expression(NO[2]/NO~"in cylinder (%)")) +
  geom_text(data = tibble(date  = cyl_on + days(20),                                           # <<< NEW: line equations, top left
                          y     = c(69),
                          label = c(sprintf("italic(x) == %.3f + %.3f~(italic(t) - italic(t)[0])", a, b))),
            aes(date, y, label = label), parse = TRUE, hjust = 0, size = 4.2, inherit.aes = FALSE) +
  theme_bw() +
  theme(
    strip.text             = element_text(size = 14),
    axis.title.x           = element_text(size = 14),
    axis.title.y           = element_text(size = 14),
    axis.text              = element_text(size = 12),
    axis.title.y.right     = element_text(size = 14, color = "black"),
    axis.text.y.right      = element_text(size = 12, color = "black"), 
    legend.position = "top"
  ) 

print(fig2)

ggsave(path = "plots/", plot = fig2, device = "png", filename = "cylinder_degrade.png", 
       dpi = 300, width = 10, height = 8, bg = "transparent")

# Fig 3. Sensitivity correction factor applied (linear) and the loess alternative
fig3 <- tibble(date = seq(cyl_on, cyl_off, length.out = 300)) %>%
  mutate(years = as.numeric(difftime(date, cyl_on, units = "days")) / 365.25,
         Linear = f_corr(years), Loess = f_loess(years)) %>%
  pivot_longer(c(Linear, Loess), names_to = "fit", values_to = "factor") %>%
  ggplot(aes(date, factor, colour = fit)) +
  geom_line(linewidth = 0.8) +
  annotate("pointrange", x = lab_date, y = cyl_cert / no_end,
           ymin = 1 + x_lab_range[1], ymax = 1 + x_lab_range[2], colour = "red") +
  scale_colour_manual(values = fit_cols, name = NULL) +
  labs(x = NULL, y = "Sensitivity correction factor (1 + x)") +
  theme_bw() + theme(legend.position = "top")
print(fig3)

if (save_figs) {
  ggsave(file.path(fig_dir, "cylinder_lab_calibration.png"), fig1, width = 5,  height = 5,   dpi = 300)
  ggsave(file.path(fig_dir, "cylinder_no2_no.png"),          fig2, width = 8,  height = 4.5, dpi = 300)
  ggsave(file.path(fig_dir, "cylinder_correction_factor.png"), fig3, width = 8, height = 4.5, dpi = 300)
}
