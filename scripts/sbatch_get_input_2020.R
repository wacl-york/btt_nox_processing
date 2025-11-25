library(DBI)
library(here)
library(dplyr)
library(tidyr)
library(lubridate)

user = system("echo $USER", intern = T)

outputFile = "/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/logs/make_ec_inputs_2020/%x_%j_%a.log" #change to hardcoded path
errFile = "/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/logs/make_ec_inputs_2020/%x_%j_%a.err"

data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/five_hz/2020"

files = tibble(
  fileName = system(paste("find", data_root, " -type f -name '*.csv'"), intern = TRUE) %>% 
    sort() %>% 
    basename()) %>% 
  mutate(fileDate = as.POSIXct(fileName, format = "NOx_5Hz_%y_%m_%d_%H%M%S.csv", tz = "UTC"),
         idx = row_number()-1) %>% 
  filter(fileDate  >= ymd_hms("2020-01-01 00:00:00"))

arrayRange = range(files$idx)


message = c("#!/usr/bin/env bash",
            "#SBATCH --job-name=make_ec_inputs # Job name",
            "#SBATCH --ntasks=1                      # Number of MPI tasks to request",
            "#SBATCH --cpus-per-task=1               # Number of CPU cores per MPI task",
            "#SBATCH --mem=10G                      # Total memory to request",
            "#SBATCH --time=0-00:10:00               # Time limit (DD-HH:MM:SS)",
            "#SBATCH --account=chem-cmde-2019        # Project account to use",
            "#SBATCH --mail-type=END,FAIL            # Mail events (NONE, BEGIN, END, FAIL, ALL)",
            paste0("#SBATCH --mail-user=cw1781@york.ac.uk   # Where to send mail"),
            paste0("#SBATCH --output=",outputFile,"       # Standard output log"),
            paste0("#SBATCH --error=",errFile,"        # Standard error log"),
            paste0("#SBATCH --array=",arrayRange[1],"-",arrayRange[2],"          # Array range"),
            "# Abort if any command fails",
            "set -e",
            "",
            "# purge any existing modules",
            "module purge",
            "",
            "# Load modules",
            "module load Apptainer/latest",
            "",
            "# Commands to run",
            paste0("apptainer exec --bind /mnt/scratch/projects/chem-cmde-2019/btt_processing/:/data/,/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/:/scripts/ /mnt/longship/projects/chem-cmde-2019/eddy4r/eddy4r.york_dev Rscript /scripts/get_flux_input_2020.R $SLURM_ARRAY_TASK_ID")
)

data_file = file(paste0('/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/sbatch/get_inputs_2020.sbatch'), open = "wt")
writeLines(message, con = data_file)
close(data_file)

