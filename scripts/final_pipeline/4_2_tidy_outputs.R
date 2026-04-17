library(cli)
library(DBI)
library(dplyr)

source("/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/final_pipeline/utils.R")
source("/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/final_pipeline/ec_tidy.R")

# Prepare -----------------------------------------------------------------

# --- get year from command line ---
args <- commandArgs(trailingOnly = TRUE)
year_arg <- as.integer(args[1])

if (is.na(year_arg) || !year_arg %in% 2020:2025) {
  stop("Please provide a valid year (2020-2025) as a command-line argument")
}

runID           <- paste0("standard_", year_arg)
inputPath       <- paste0("/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_2/out/BTT/standard_", year_arg)
boundOutputPath <- paste0("/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_2/out/BTT/standard_", year_arg, "_bound")

analysisText <- "NOx_5Hz"
types <- c("mean", "ACF", "error", "isca", "foot", "spec", "itc", "stna", "lod", "lagTimes")

tempEcCollatedOutputs <- here::here('tmp', 'tmpEcCollatedOutputs.RDS')
tempSbatch            <- here::here('tmp', 'tempSbatch')
paraPathBound         <- file.path(boundOutputPath, paste0(runID, "_para.RDS"))

# Slurm Settings
user         <- system("echo $USER", intern = TRUE)
account_code <- "chem-cmde-2019"

con = connect_to_db(FALSE)

# does this input path exist?
if (!dir.exists(inputPath)) {
  
  if (dir.exists(boundOutputPath)) {
    boundOutputList <- list_ec_outputs(boundOutputPath)
    
    if (nrow(boundOutputList) == 0) {
      DBI::dbDisconnect(con, shutdown = TRUE)
      stop("Could not find normal or bound inputs")
    } else {
      message("Found bound inputs but no normal ones - skipping binding step and reattempting database write")
    }
  }
  
} else {
  
  # Make required dirs ------------------------------------------------------
  
  if (!dir.exists(here::here("tmp"))) dir.create(here::here("tmp"))
  if (!dir.exists(boundOutputPath))   dir.create(boundOutputPath)
  
  # Collate files to tidy ---------------------------------------------------
  
  ecOutputs <- list_ec_outputs(inputPath)
  
  ecOutputsCollated <- collate_ec_outputs(
    ecOutputs        = ecOutputs,
    outputDirectory  = boundOutputPath,
    analysisText     = analysisText
  )
  
  saveRDS(ecOutputsCollated, tempEcCollatedOutputs)
  
  # copy para file to bound directory root
  paraPath <- ecOutputs |>
    filter(fileType == "para") |>
    pull(filePath) |>
    head(1)
  
  para <- readRDS(paraPath)
  saveRDS(para, paraPathBound)
  
  # Submit Array Job --------------------------------------------------------
  
  resp <- submit_ec_tidy_job(
    runID                 = runID,
    user                  = user,
    account_code          = account_code,
    tempEcCollatedOutputs = tempEcCollatedOutputs,
    types                 = types
  )
  
  jobid <- as.numeric(stringr::str_remove(resp, "Submitted batch job "))
  
  # Monitor Job Progress ----------------------------------------------------
  
  spinner <- cli::make_spinner(template = "Waiting for array job to complete {spin}")
  
  while (!is_job_complete(jobid)) {
    for (i in 1:39) {
      spinner$spin()
      Sys.sleep(0.25)
    }
    spinner$spin()
    Sys.sleep(0.25)
  }
  
  jobStatus <- get_job_status(jobid)
  
  if (!(length(jobStatus) == 1 & "COMPLETED" %in% jobStatus)) {
    stop(paste0("Job finished with status: ", paste(jobStatus, collapse = ", ")))
  }
}

# Write to duckdb ---------------------------------------------------------

message("Collating bound files for database write")

para       <- readRDS(paraPathBound)
toLoadtoDB <- ec_parquet_load_list(boundOutputPath, runID)

message("Writing to db")
cli_progress_bar(total = nrow(toLoadtoDB), name = "Load EC data to db")

for (i in 1:nrow(toLoadtoDB)) {
  
  cli_progress_update(status = toLoadtoDB$tableName[i])
  
  if (dbExistsTable(con, toLoadtoDB$tableName[i])) {
    dbRemoveTable(con, toLoadtoDB$tableName[i])
  }
  
  DBI::dbExecute(
    con,
    glue::glue(
      "
      CREATE TABLE {{toLoadtoDB$tableName[i]}} AS
      SELECT *
      FROM read_parquet('{{toLoadtoDB$loadPath[i]}}', hive_partitioning = true)
      ",
      .open = "{{", .close = "}}", sep = ""
    )
  )
}

DBI::dbDisconnect(con, shutdown = TRUE)

# Cleanup -----------------------------------------------------------------

unlink(here::here('tmp'), recursive = TRUE)
#unlink(inputPath,         recursive = TRUE)