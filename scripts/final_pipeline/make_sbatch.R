library(DBI)
library(here)
library(dplyr)
library(tidyr)
library(lubridate)

# --- get year from command line ---
args <- commandArgs(trailingOnly = TRUE)
year_arg <- as.integer(args[1])

if (is.na(year_arg) || !year_arg %in% 2020:2025) {
  stop("Please provide a valid year (2020-2025) as a command-line argument")
}

user <- system("echo $USER", intern = TRUE)

log_dir  <- paste0("/mnt/scratch/users/cw1781/btt_cal_processing/logs/make_ec_inputs2_", year_arg)
outputFile <- paste0(log_dir, "/%x_%j_%a.log")
errFile    <- paste0(log_dir, "/%x_%j_%a.err")

data_root <- paste0("/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/five_hz/", year_arg)

files <- tibble(
  fileName = system(paste("find", data_root, "-type f -name '*.csv'"), intern = TRUE) %>%
    sort() %>%
    basename()
) %>%
  mutate(
    fileDate = as.POSIXct(fileName, format = "NOx_5Hz_%y_%m_%d_%H%M%S.csv", tz = "UTC"),
    idx = row_number() - 1
  ) %>%
  filter(fileDate >= ymd_hms(paste0(year_arg, "-01-01 00:00:00")))

arrayRange <- range(files$idx)

message <- c(
  "#!/usr/bin/env bash",
  "#SBATCH --job-name=make_ec_inputs",
  "#SBATCH --ntasks=1",
  "#SBATCH --cpus-per-task=1",
  "#SBATCH --mem=10G",
  "#SBATCH --time=0-00:10:00",
  "#SBATCH --account=chem-cmde-2019",
  "#SBATCH --mail-type=END,FAIL",
  paste0("#SBATCH --mail-user=cw1781@york.ac.uk"),
  paste0("#SBATCH --output=", outputFile),
  paste0("#SBATCH --error=",  errFile),
  paste0("#SBATCH --array=",  arrayRange[1], "-", arrayRange[2]),
  "set -e",
  "",
  "module purge",
  "module load Apptainer/latest",
  "",
  paste0(
    "apptainer exec ",
    "--bind /mnt/scratch/projects/chem-cmde-2019/btt_processing/:/data/,",
    "/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/:/scripts/ ",
    "/mnt/longship/projects/chem-cmde-2019/eddy4r/eddy4r.york_dev ",
    "Rscript /scripts/final_pipeline/make_ec_inputs_3_2.R $SLURM_ARRAY_TASK_ID ", year_arg
  )
)

# create log dir if needed
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

sbatch_file <- paste0(
  "/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/final_pipeline/get_inputs_",
  year_arg, ".sbatch"
)

con <- file(sbatch_file, open = "wt")
writeLines(message, con = con)
close(con)

message("Written: ", sbatch_file)
message("Array range: ", arrayRange[1], "-", arrayRange[2], " (", nrow(files), " files)")