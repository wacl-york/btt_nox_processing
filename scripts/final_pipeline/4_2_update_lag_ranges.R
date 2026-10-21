# =====================================================================
# update_lag_ranges.R
#
# Keeps the existing lag ranges (periodLagRanges in the duckdb) for the periods they already cover, and adds
# ranges for the periods that have none yet (end of 2025 - 2026), from a first-pass (unconstrained) run of
# those hours only. Same method as before: best lag per hour = largest |ACF| between LAG_MIN and LAG_MAX,
# period median +/- 2 s.
# Writes lag_ranges_v2.csv (old + new periods), which flux_run.R reads for the constrained run.
# =====================================================================
library(tidyverse)
library(arrow)
library(duckdb)

old_proc <- "/mnt/longship/projects/chem-cmde-2019/btt_processing/processing"     # original runs (read only)
new_proc <- "/mnt/scratch/projects/chem-bttao-2026/btt_processing/processing"    # v2 runs
db_path  <- file.path(old_proc, "ec_2/duckdb/ec_BT.duckdb")                       # existing lag ranges
out_root <- file.path(new_proc, "ec_2/out/BTT")                                   # v2 first-pass output
lag_file <- file.path(new_proc, "ec_2/lag_ranges_v2.csv")    # = /processing/ec_2/lag_ranges_v2.csv in the container

LAG_MIN       <- -15
LAG_MAX       <-  -2
lag_halfwidth <-   2
fast_hz       <-   5
min_hours     <-  50     # a period needs at least this many hours with a best lag, else no range (wide window used)
species <- c("rtioMoleDryNO" = "NO", "rtioMoleDryNO2" = "NO2", "rtioMoleDryco2" = "CO2")

# first-pass runs that cover the new hours (runIDs as named by flux_run.R)
new_runs <- c("2025" = "v2_unconstrained_2025", "2026" = "v2_unconstrained_2026")

# periods to (re)define from the first pass - set after looking at the existing table in step 1
update_periods <- c("period12", "period13")

get_period_4_lags <- function(datetime) {
  case_when(
    datetime <= as.POSIXct("2020-10-13 01:00:00", tz = "UTC") ~ "period1",
    datetime <  as.POSIXct("2021-01-11", tz = "UTC") ~ "period2",
    datetime <  as.POSIXct("2021-06-11", tz = "UTC") ~ "period3",
    datetime <  as.POSIXct("2021-12-31", tz = "UTC") ~ "period4",
    datetime <  as.POSIXct("2022-11-01", tz = "UTC") ~ "period5",
    datetime <  as.POSIXct("2023-10-19", tz = "UTC") ~ "period6",
    datetime <  as.POSIXct("2024-02-08", tz = "UTC") ~ "period7",
    datetime <  as.POSIXct("2025-02-03", tz = "UTC") ~ "period8",
    datetime <  as.POSIXct("2025-03-18", tz = "UTC") ~ "period9",
    datetime <  as.POSIXct("2025-06-05", tz = "UTC") ~ "period10",
    datetime <  as.POSIXct("2025-11-13", tz = "UTC") ~ "period11",
    datetime <  as.POSIXct("2025-12-30", tz = "UTC") ~ "period12",
    datetime <= as.POSIXct("2026-12-30", tz = "UTC") ~ "period13",
    TRUE ~ NA_character_)
}


# ==== 1. EXISTING LAG RANGES ====
con <- dbConnect(duckdb(), dbdir = db_path, read_only = TRUE)
old_ranges <- tbl(con, "periodLagRanges") |> collect()
dbDisconnect(con, shutdown = TRUE)

print(old_ranges, n = Inf, width = Inf)


# ==== 2. NEW PERIODS FROM THE FIRST PASS ====
# ==== 2. NEW PERIODS FROM THE FIRST PASS ====
# ACF files are read straight from each hour's output folder (no tidy/binding step needed)
read_acf <- function(year, run_id) {                                                             # <<< CHANGED
  files <- list.files(file.path(out_root, run_id), pattern = "_ACF", recursive = TRUE, full.names = TRUE)
  message(year, ": ", length(files), " ACF files")
  if (length(files) == 0) return(NULL)
  map_df(files, possibly(\(f) read_csv(f, show_col_types = FALSE) |> select(unixTimeMin, name, lag, acf),
                         otherwise = NULL)) |>
    filter(name %in% names(species)) |>
    distinct(unixTimeMin, name, lag, .keep_all = TRUE) |>   # in case an hour's ACF is saved twice
    group_by(unixTimeMin, name) |>
    filter(row_number() < 200) |>                           # as before
    ungroup()
}

acf_new <- imap_dfr(new_runs, \(run_id, year) read_acf(year, run_id)) |>
  mutate(date = as.POSIXct(unixTimeMin, origin = "1970-01-01", tz = "UTC"),
         lag_sec = lag / fast_hz,
         period  = get_period_4_lags(date)) |>
  filter(between(lag_sec, LAG_MIN, LAG_MAX), period %in% update_periods, !is.na(acf))

best_lags <- acf_new |>
  group_by(unixTimeMin, name) |>
  slice_max(abs(acf), n = 1, with_ties = FALSE) |>
  ungroup()

new_ranges <- best_lags |>
  group_by(period, name) |>
  summarise(date_start = min(date), date_end = max(date), n_hours = n(),
            median_lag = median(lag_sec), .groups = "drop") |>
  filter(n_hours >= min_hours) |>
  mutate(type   = unname(species[name]),
         # same order as the existing table: lagMin is the end nearer zero, lagMax the more negative end
         lagMin = median_lag + lag_halfwidth,
         lagMax = median_lag - lag_halfwidth)

print(new_ranges |> select(period, type, n_hours, date_start, date_end, median_lag, lagMin, lagMax))


# ==== 3. COMBINE AND WRITE ====
keep_cols <- c("period", "type", "date_start", "date_end", "median_lag", "lagMin", "lagMax")

lag_ranges <- bind_rows(
  old_ranges |> filter(!period %in% update_periods) |> select(any_of(keep_cols)) |> mutate(source = "existing"),
  new_ranges |> select(any_of(keep_cols)) |> mutate(source = "v2 first pass")
) |>
  arrange(as.numeric(str_remove(period, "period")), type)

print(lag_ranges, n = Inf)

missing <- expand_grid(period = paste0("period", 1:13), type = unname(species)) |>
  anti_join(lag_ranges, by = c("period", "type"))
if (nrow(missing) > 0) { message("No lag range for (the wide window will be used):"); print(missing, n = Inf) }

write_csv(lag_ranges, lag_file)
message("Written: ", lag_file)


# ==== 4. CHECK FIGURE FOR THE NEW PERIODS ====
fig_new <- ggplot(acf_new) +
  geom_tile(aes(date, lag_sec, fill = acf)) +
  geom_rect(data = new_ranges, aes(xmin = date_start, xmax = date_end, ymin = lagMax, ymax = lagMin),
            fill = "white", colour = "white", alpha = 0.3) +
  geom_point(data = best_lags, aes(date, lag_sec), size = 0.5, colour = "#9E2C49") +
  scale_fill_viridis_c() +
  facet_wrap(~name, ncol = 1, scales = "free_y",
             labeller = as_labeller(c(rtioMoleDryNO = "NO", rtioMoleDryNO2 = "NO[2]", rtioMoleDryco2 = "CO[2]"),
                                    label_parsed)) +
  theme_minimal() +
  labs(x = NULL, y = "Lag (s)", fill = "ACF", caption = "Points = best lag each hour; boxes = median +/- 2 s")

print(fig_new)



f <- list.files("/mnt/scratch/projects/chem-bttao-2026/btt_processing/processing/ec_2/out/BTT/v2_constrained_2023",
                pattern = "_ACF", recursive = TRUE, full.names = TRUE)[1]


readr::read_csv(f, show_col_types = FALSE) |>
  dplyr::group_by(name) |>
  dplyr::summarise(min_lag_s = min(lag) / 5, max_lag_s = max(lag) / 5, n_lags = dplyr::n())
