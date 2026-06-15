source("/scripts/final_pipeline/utils.R")

FILE_SELECT <- Sys.getenv("FILE_SELECT")
print(FILE_SELECT)

# Get the date of this file from the filename
file_date <- as.POSIXct(
  stringr::str_remove(basename(FILE_SELECT), ".csv"),
  format = "NOx_5Hz_%y_%m_%d_%H%M%S",
  tz = "UTC"
)


file_period <- get_period(file_date)

cat("File date:", format(file_date), "\n")
cat("File period:", file_period, "\n")

con <- DBI::dbConnect(
  drv       = duckdb::duckdb(),
  dbdir     = "/processing/ec_2/duckdb/ec_BT.duckdb",
  read_only = TRUE
)

lagRanges <- tbl(con, "periodLagRanges") |>
  collect() |>
  filter(period == file_period)

DBI::dbDisconnect(con, shutdown = TRUE)

cat("Lag ranges found:", nrow(lagRanges), "rows\n")


# Extract lag ranges per species — one row each
lag_NO  <- lagRanges |> filter(type == "NO")
lag_NO2 <- lagRanges |> filter(type == "NO2")
lag_CO2 <- lagRanges |> filter(type == "CO2")


para = eddy4R.york::def.para(
  DirWrk = "/processing/ec_2",
  DirInp = "in/2024",
  siteName = "BTT",
  analysis = stringr::str_remove(basename(Sys.getenv("FILE_SELECT")), ".csv"),
  runID = "constrain_lags_2024",
  fileMask = "NOx_5Hz_%y_%m_%d_%H%M%S.csv",
  species = c("NO","NO2","co2"),
  aggregationPeriod = 3600,
  fileDuration = 3600,
  lat = 51.5215, #CHECK
  AlgBase = "trnd",
  idepVar = "unixTime",
  MethRot = "double",
  missingMethod = "mean",
  lagApplyCorrection = TRUE,
  lagApplyRangeLimit = TRUE,
  lagNOc = TRUE,
  writeFastData = TRUE,
  # lagRangeLimit = list(c(0,0),
  #                      c(0,0),
  #                      c(0,-10),
  #                      c(0,-10),
  #                      c(0,-10)
  # ),
  lagRangeLimit = list(
    c(0, 0),
    c(0, 0),
    c(lag_NO$lagMin,  lag_NO$lagMax),
    c(lag_NO2$lagMin, lag_NO2$lagMax),
    c(lag_CO2$lagMin, lag_CO2$lagMax)
  ),
  lagDefaults = c(
    0, 0,
    lag_NO$median_lag,
    lag_NO2$median_lag,
    lag_CO2$median_lag
  )
)

# pass the (in-container) path as the FILE_SELECT environment variable
matching = para$filePaths %in% Sys.getenv("FILE_SELECT")
para$filePaths = para$filePaths[matching]
para$fileNames = para$fileNames[matching]

eddy4R.york::wrap.towr(para)