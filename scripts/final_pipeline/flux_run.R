# =====================================================================
# flux_run.R  -  one hour of eddy-covariance fluxes with eddy4R.york
#
# Environment variables (set by get_flux.sbatch):
#   FILE_SELECT  in-container path of the hourly input file
#   LAG_MODE     "unconstrained" - first pass: lag searched over the wide window, used only to find the lags
#                "constrained"   - final run: lag limited to the period's range in lag_ranges_v2.csv
# =====================================================================
source("/scripts/final_pipeline/utils.R")   # as before (libraries, helpers)
library(tidyverse)

FILE_SELECT <- Sys.getenv("FILE_SELECT")
LAG_MODE    <- Sys.getenv("LAG_MODE", "constrained")
print(FILE_SELECT); print(LAG_MODE)

lag_file <- "/processing/ec_2/lag_ranges_v2.csv"     # written by update_lag_ranges.R
LAG_MIN  <- -15                                       # wide search window (s), first pass and fallback
LAG_MAX  <-  -2
LAG_DEF  <-  -8

# lag periods - same as update_lag_ranges.R and the dates in periodLagRanges
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

# date, year and lag period of this file (from its name)
file_date   <- as.POSIXct(str_remove(basename(FILE_SELECT), "\\.csv$"),
                          format = "NOx_5Hz_%y_%m_%d_%H%M%S", tz = "UTC")
year_val    <- format(file_date, "%Y")
file_period <- get_period_4_lags(file_date)
cat("File date:", format(file_date), " period:", file_period, "\n")


# ---- lag ranges ----
# same order as periodLagRanges: lagMin = end nearer zero, lagMax = more negative end, e.g. c(-4.2, -8.2)
wide <- tibble(lagMin = LAG_MAX, lagMax = LAG_MIN, median_lag = LAG_DEF)

if (LAG_MODE == "unconstrained") {
  lag_NO <- lag_NO2 <- lag_CO2 <- wide
} else {
  lag_ranges <- read_csv(lag_file, show_col_types = FALSE) |> filter(period == file_period)
  # one row per species; if a species has none (e.g. no CO2 in that period) use the wide window
  get_lag <- function(sp) {
    r <- lag_ranges |> filter(type == sp)
    if (nrow(r) == 1) return(r)
    warning("No single lag range for ", sp, " in ", file_period, " - using the wide window")
    wide
  }
  lag_NO  <- get_lag("NO")
  lag_NO2 <- get_lag("NO2")
  lag_CO2 <- get_lag("CO2")
}
print(bind_rows(NO = lag_NO, NO2 = lag_NO2, CO2 = lag_CO2, .id = "species") |> select(species, lagMin, lagMax, median_lag))


# ---- eddy4R ----
para <- eddy4R.york::def.para(
  DirWrk   = "/processing/ec_2",
  DirInp   = paste0("in/", year_val),
  siteName = "BTT",
  analysis = str_remove(basename(FILE_SELECT), "\\.csv$"),
  runID    = paste0("v2_", LAG_MODE, "_", year_val),       # e.g. v2_unconstrained_2024, v2_constrained_2024
  fileMask = "NOx_5Hz_%y_%m_%d_%H%M%S.csv",
  species  = c("NO", "NO2", "co2"),
  aggregationPeriod = 3600,
  fileDuration      = 3600,
  lat      = 51.5215,
  AlgBase  = "trnd",
  idepVar  = "unixTime",
  MethRot  = "double",
  missingMethod      = "mean",
  lagApplyCorrection = TRUE,
  lagApplyRangeLimit = TRUE,
  lagNOc             = TRUE,
  writeFastData      = LAG_MODE == "constrained",          # first pass only needs the lags/ACF
  lagRangeLimit = list(
    c(0, 0),
    c(0, 0),
    c(lag_NO$lagMin,  lag_NO$lagMax),
    c(lag_NO2$lagMin, lag_NO2$lagMax),
    c(lag_CO2$lagMin, lag_CO2$lagMax)
  ),
  lagDefaults = c(0, 0, lag_NO$median_lag, lag_NO2$median_lag, lag_CO2$median_lag)
)

# run only this file (matched by name, so the path format doesn't matter)
matching <- basename(para$filePaths) == basename(FILE_SELECT)
if (sum(matching) != 1) stop("FILE_SELECT matched ", sum(matching), " input files: ", FILE_SELECT)
para$filePaths <- para$filePaths[matching]
para$fileNames <- para$fileNames[matching]

eddy4R.york::wrap.towr(para)