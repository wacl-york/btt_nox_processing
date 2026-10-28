# =====================================================================
# 4_3_tidy_outputs.R
#
# Binds the hourly eddy4R output of one year of the v2 (recalibrated) constrained run into
# year/month parquet files, and loads them into a new duckdb (the original database is not touched).
#
# usage (on a login node, from final_pipeline):
#   Rscript 4_3_tidy_outputs_v2.R <year> [bind|load|all]
#     bind - collate the hourly files and run the binding array job (years can run at the same time)
#     load - write the bound parquet of that year into the v2 duckdb 
#     all  - bind then load (default)
# =====================================================================
library(cli)
library(DBI)
library(dplyr)

source("/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/final_pipeline/utils.R")
source("/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/final_pipeline/ec_tidy.R")

# Prepare -----------------------------------------------------------------

args     <- commandArgs(trailingOnly = TRUE)
year_arg <- as.integer(args[1])
mode     <- if (length(args) >= 2) args[2] else "all"

if (is.na(year_arg) || !year_arg %in% 2020:2026) {
  stop("Please provide a valid year (2020-2026) as a command-line argument")
}
if (!mode %in% c("bind", "load", "all")) stop("Second argument must be bind, load or all")

out_root        <- "/mnt/scratch/projects/chem-bttao-2026/btt_processing/processing/ec_2/out/BTT"
runID           <- paste0("v2_constrained_", year_arg)
inputPath       <- file.path(out_root, runID)
boundOutputPath <- file.path(out_root, paste0(runID, "_bound"))


# chapter scripts only need connect_to_db() -> connect_to_db_v2()
tablePrefix <- paste0("constrain_lags_", year_arg)

analysisText <- "NOx_5Hz"
types <- c("mean", "ACF", "error", "isca", "foot", "spec", "itc", "stna", "lod", "lagTimes")

tmpDir                <- here::here("tmp", runID)
tempEcCollatedOutputs <- file.path(tmpDir, "tmpEcCollatedOutputs.RDS")
tempSbatch            <- file.path(tmpDir, "tempSbatch")
paraPathBound         <- file.path(boundOutputPath, paste0(runID, "_para.RDS"))

user         <- system("echo $USER", intern = TRUE)
account_code <- "chem-cmde-2019"


# Bind --------------------------------------------------------------------

if (mode %in% c("bind", "all")) {
  
  if (!dir.exists(inputPath)) stop("No hourly output found at ", inputPath)
  
  dir.create(tmpDir, recursive = TRUE, showWarnings = FALSE)
  dir.create(boundOutputPath, showWarnings = FALSE)
  
  message("Listing hourly output files in ", inputPath)
  ecOutputs <- list_ec_outputs(inputPath)
  print(count(ecOutputs, fileType), n = Inf)
  
  ecOutputsCollated <- collate_ec_outputs(
    ecOutputs       = ecOutputs,
    outputDirectory = boundOutputPath,
    analysisText    = analysisText
  )
  saveRDS(ecOutputsCollated, tempEcCollatedOutputs)
  
  # copy para file to bound directory root
  paraPath <- ecOutputs |> filter(fileType == "para") |> pull(filePath) |> head(1)
  saveRDS(readRDS(paraPath), paraPathBound)
  
  # submit the binding array job (one task per file type and month)
  resp  <- submit_ec_tidy_job(
    runID                 = runID,
    user                  = user,
    account_code          = account_code,
    tempEcCollatedOutputs = tempEcCollatedOutputs,
    types                 = types,
    tempSbatch            = tempSbatch
  )
  jobid <- suppressWarnings(as.numeric(stringr::str_remove(resp, "Submitted batch job ")))
  if (length(jobid) != 1 || is.na(jobid))
    stop("sbatch did not submit the binding job: ", paste(resp, collapse = " "))
  message("Binding job ", jobid, " submitted (", nrow(ecOutputsCollated), " tasks)")
  
  spinner <- cli::make_spinner(template = "Waiting for array job to complete {spin}")
  while (!is_job_complete(jobid)) {
    for (i in 1:40) { spinner$spin(); Sys.sleep(0.25) }
  }
  
  jobStatus <- get_job_status(jobid)
  if (!(length(jobStatus) == 1 && "COMPLETED" %in% jobStatus)) {
    stop(paste0("Job finished with status: ", paste(jobStatus, collapse = ", ")))
  }
  message("Binding complete: ", boundOutputPath)
  
  unlink(tmpDir, recursive = TRUE)
  # unlink(inputPath, recursive = TRUE)   # hourly output is kept; move it to longship once checked
}


# Load into the v2 duckdb -------------------------------------------------

if (mode %in% c("load", "all")) {
  
  if (!dir.exists(boundOutputPath)) stop("No bound output found at ", boundOutputPath)
  
  toLoadtoDB <- ec_parquet_load_list(boundOutputPath, tablePrefix)
  print(toLoadtoDB, n = Inf)
  
  con <- connect_to_db_v2(FALSE)
  
  cli_progress_bar(total = nrow(toLoadtoDB), name = "Load EC data to db")
  for (i in seq_len(nrow(toLoadtoDB))) {
    cli_progress_update(status = toLoadtoDB$tableName[i])
    if (dbExistsTable(con, toLoadtoDB$tableName[i])) dbRemoveTable(con, toLoadtoDB$tableName[i])
    DBI::dbExecute(con, glue::glue(
      "CREATE TABLE {{toLoadtoDB$tableName[i]}} AS
       SELECT * FROM read_parquet('{{toLoadtoDB$loadPath[i]}}', hive_partitioning = true, union_by_name = true)",
      .open = "{{", .close = "}}"))
  }
  cli_progress_done()
  DBI::dbDisconnect(con, shutdown = TRUE)
  message("Loaded ", nrow(toLoadtoDB), " tables for ", year_arg)
}