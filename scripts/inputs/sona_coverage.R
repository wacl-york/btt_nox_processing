# ==============================================================================
# Sona data coverage — two questions:
#   1. Which hourly files exist?
#   2. Within files that exist, how much h2o_ppthou is non-NA?
#
# Run this before deciding whether SAM fallback for H2O is worth the complexity.
# ==============================================================================

library(tidyverse)

sona_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/sona_data"

# ------------------------------------------------------------------------------
# 1. SCAN FILES
# ------------------------------------------------------------------------------

all_sona <- tibble(
  path = list.files(sona_root, pattern = "\\.sona$", recursive = TRUE, full.names = TRUE)
) %>%
  mutate(
    fname    = basename(path),
    dt_str   = stringr::str_extract(fname, "\\d{8}_\\d{4}"),
    datetime = lubridate::ymd_hm(dt_str, tz = "UTC"),
    date     = as.Date(datetime),
    hour     = lubridate::hour(datetime)
  ) %>%
  filter(!is.na(datetime))

# Full expected hourly grid
all_dates  <- seq(min(all_sona$date), max(all_sona$date), by = "day")
full_grid  <- expand_grid(date = all_dates, hour = 0:23)

coverage_grid <- full_grid %>%
  left_join(
    all_sona %>% select(date, hour, path) %>% mutate(file_present = TRUE),
    by = c("date", "hour")
  ) %>%
  mutate(file_present = replace_na(file_present, FALSE))

# ------------------------------------------------------------------------------
# 2. READ EACH PRESENT FILE — count h2o_ppthou completeness
# ------------------------------------------------------------------------------

message(sprintf("Reading %d sona files to check h2o completeness...", sum(coverage_grid$file_present)))

h2o_quality <- coverage_grid %>%
  filter(file_present) %>%
  mutate(
    h2o_frac = map_dbl(path, function(f) {
      tryCatch({
        df <- read_csv(f, show_col_types = FALSE)
        if (!"h2o_ppthou" %in% names(df)) return(NA_real_)
        mean(!is.na(df$h2o_ppthou))
      }, error = function(e) NA_real_)
    })
  ) %>%
  select(date, hour, file_present, h2o_frac)

# Join back to full grid
coverage_full <- coverage_grid %>%
  select(date, hour, file_present) %>%
  left_join(h2o_quality %>% select(date, hour, h2o_frac), by = c("date", "hour")) %>%
  mutate(
    # Classify each hour
    status = case_when(
      !file_present          ~ "file missing",
      is.na(h2o_frac)        ~ "file present, h2o column absent",
      h2o_frac == 0          ~ "file present, h2o all NA",
      h2o_frac < 0.5         ~ "file present, h2o >50% missing",
      h2o_frac < 1           ~ "file present, h2o some gaps",
      TRUE                   ~ "complete"
    )
  )

# ------------------------------------------------------------------------------
# 3. DAILY SUMMARY
# ------------------------------------------------------------------------------

daily <- coverage_full %>%
  group_by(date) %>%
  summarise(
    frac_files_present = mean(file_present),
    frac_h2o_complete  = mean(status == "complete"),
    frac_h2o_any       = mean(file_present & !is.na(h2o_frac) & h2o_frac > 0),
    .groups = "drop"
  )

# ------------------------------------------------------------------------------
# 4. CONSOLE SUMMARY
# ------------------------------------------------------------------------------

total_hours    <- nrow(coverage_full)
status_summary <- coverage_full %>% count(status) %>% mutate(pct = 100 * n / total_hours)

message("\n=== Hourly status breakdown ===")
print(status_summary, n = Inf)

message(sprintf(
  "\nOverall: %d hours total | %.1f%% files present | %.1f%% fully complete h2o",
  total_hours,
  100 * mean(coverage_full$file_present),
  100 * mean(coverage_full$status == "complete")
))

# ------------------------------------------------------------------------------
# 5. PLOT — three lines on one panel: file presence, any h2o, complete h2o
# ------------------------------------------------------------------------------

plot_data <- daily %>%
  pivot_longer(
    cols      = c(frac_files_present, frac_h2o_any, frac_h2o_complete),
    names_to  = "metric",
    values_to = "fraction"
  ) %>%
  mutate(metric = recode(metric,
                         frac_files_present = "File present",
                         frac_h2o_any       = "H2O any data",
                         frac_h2o_complete  = "H2O fully complete"
  ))

p <- ggplot(plot_data, aes(x = date, y = fraction, colour = metric)) +
  geom_line(linewidth = 0.8, alpha = 0.9) +
  scale_colour_manual(values = c(
    "File present"       = "#4d9de0",
    "H2O any data"       = "#e15554",
    "H2O fully complete" = "#3bb273"
  )) +
  scale_y_continuous(
    labels = scales::percent_format(accuracy = 1),
    limits = c(0, 1),
    expand = expansion(mult = c(0, 0.02))
  ) +
  scale_x_date(date_breaks = "1 month", date_labels = "%b %Y") +
  geom_hline(yintercept = 1, linetype = "dashed", colour = "grey60", linewidth = 0.4) +
  labs(
    title    = "Sona data coverage",
    subtitle = "Daily fraction of hours: file present vs h2o data quality",
    x        = NULL,
    y        = "Fraction of hours",
    colour   = NULL
  ) +
  theme_minimal(base_size = 13) +
  theme(
    legend.position    = "top",
    panel.grid.minor   = element_blank(),
    panel.grid.major.x = element_blank(),
    axis.text.x        = element_text(angle = 35, hjust = 1),
    plot.title         = element_text(face = "bold")
  )

ggsave("sona_coverage.png", plot = p, width = 13, height = 5, dpi = 180)
message("Plot saved: sona_coverage.png")

# Optional: save the full hourly status table for inspection
write_csv(coverage_full, "sona_coverage_hourly.csv")
message("Hourly status table saved: sona_coverage_hourly.csv")
