#!/usr/bin/env Rscript
#SBATCH --job-name=tidy_ec_1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G
#SBATCH --time=0-00:30:00
#SBATCH --account=chem-cmde-2019
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cw1781@york.ac.uk
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sensitivity_bind/test0/%x_%j.log
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/sensitivity_bind/test0/%x_%j.err

library(cli)
library(DBI)
library(purrr)
library(dplyr)
library(stringr)
library(lubridate)
library(readr)
library(tidyr)

base_out <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/sensitivity/out_bound/BTT/test_1"

outputs <- tibble(
  filePath = system(
    paste(
      "find",
      "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/sensitivity/out/BTT/test_1",
      "-type f"
    ),
    intern = TRUE
  )
) |>
  mutate(
    fileName = basename(filePath),
    fileType = case_when(
      str_detect(fileName, "_max") ~ "max",
      str_detect(fileName, "_isca") ~ "isca",
      str_detect(fileName, "_data") ~ "data",
      str_detect(fileName, "_conv") ~ "conv",
      str_detect(fileName, "_foot") ~ "foot",
      str_detect(fileName, "_base") ~ "base",
      str_detect(fileName, "_ACF") ~ "ACF",
      str_detect(fileName, "_diff") ~ "diff",
      str_detect(fileName, "_spec") ~ "spec",
      str_detect(fileName, "_lod") ~ "lod",
      str_detect(fileName, "_lagTimes") ~ "lagTimes",
      str_detect(fileName, "_logfile") ~ "logfile",
      str_detect(fileName, "_itc") ~ "itc",
      str_detect(fileName, "_error") ~ "error",
      str_detect(fileName, "_stna") ~ "stna",
      str_detect(fileName, "_sd") ~ "sd",
      str_detect(fileName, "_para") ~ "para",
      str_detect(fileName, "_mtrxRot01") ~ "mtrxRot01",
      str_detect(fileName, "_min") ~ "min",
      str_detect(fileName, "_corr") ~ "corr",
      str_detect(fileName, "_mean") ~ "mean"
    ),
    fileExtension = case_when(
      str_detect(fileName, "\\.csv\\.gz$") ~ ".csv.gz",
      str_detect(fileName, "\\.csv$") ~ ".csv",
      str_detect(fileName, "\\.RDS$") ~ ".RDS",
      str_detect(fileName, "\\.txt$") ~ ".txt"
    )
  )

outputsToLoad <- outputs |>
  filter(fileType %in% c(
    "mean",
   # "ACF",
    "error",
   # "isca",
   # "foot",
   # "spec",
  #  "itc",
    "stna"
  )) |>
  mutate(
    fileMask = case_when(
      fileExtension == ".csv" ~
        paste0("NOx_5Hz_%y_%m_%d_%H%M%S_", fileType, fileExtension),
      fileExtension == ".csv.gz" ~
        paste0("%Y%m%d_%H_NOx_5Hz_%y_%m_%d_%H%M%S_", fileType, fileExtension)
    ),
    fileDate = as.POSIXct(fileName, format = fileMask, tz = "UTC"),
    yr = year(fileDate),
    mnth = str_pad(month(fileDate), width = 2, side = "left", pad = "0"),
    outDir = file.path(base_out, yr, mnth),
    outPath = file.path(
      outDir,
      paste0(yr, "_", mnth, "_", fileType, fileExtension)
    )
  ) |>
  arrange(fileDate) |>
  nest_by(outPath, outDir)

message("Found ", nrow(outputsToLoad), " output groups.")

for (i in seq_len(nrow(outputsToLoad))) {
  
  message(
    "Processing ", i, " of ", nrow(outputsToLoad),
    ": ", basename(outputsToLoad$outPath[[i]])
  )
  
  dat <- map_df(
    outputsToLoad$data[[i]]$filePath,
    ~read_csv(.x, show_col_types = FALSE)
  )
  
  if (!dir.exists(outputsToLoad$outDir[[i]])) {
    dir.create(outputsToLoad$outDir[[i]], recursive = TRUE)
  }
  
  write_csv(dat, outputsToLoad$outPath[[i]])
  
  message("Finished: ", outputsToLoad$outPath[[i]])
}

message("All files processed.")

