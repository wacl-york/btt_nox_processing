library(DBI)
library(here)
library(dplyr)
library(tidyr)
library(lubridate)

source(here::here('functions','utils.R'))

user = system("echo $USER", intern = T)

outputFile = logs_path("make_ec_inputs","%x_%j_%a.log") #change to hardcoded path
errFile = logs_path("make_ec_inputs","%x_%j_%a.err")

message = c("#!/usr/bin/env bash",
            "#SBATCH --job-name=make_ec_inputs # Job name",
            "#SBATCH --ntasks=1                      # Number of MPI tasks to request",
            "#SBATCH --cpus-per-task=1               # Number of CPU cores per MPI task",
            "#SBATCH --mem=2G                      # Total memory to request",
            "#SBATCH --time=0-00:10:00               # Time limit (DD-HH:MM:SS)",
            "#SBATCH --account=chem-cmde-2019        # Project account to use",
            "#SBATCH --mail-type=END,FAIL            # Mail events (NONE, BEGIN, END, FAIL, ALL)",
            paste0("#SBATCH --mail-user=cw1781@york.ac.uk   # Where to send mail"),
            paste0("#SBATCH --output=",outputFile,"       # Standard output log"),
            paste0("#SBATCH --error=",errFile,"        # Standard error log"),
            paste0("#SBATCH --array=0-",nrow(ts)-1,"          # Array range"),
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
            paste0("apptainer exec --cwd /btt_processing --bind /mnt/scratch/projects/chem-cmde-2019/btt_processing/:/btt_processing /mnt/longship/projects/chem-cmde-2019/eddy4r/eddy4r.york_dev Rscript /btt_processing/scripts/make_ec_inputs.R $SLURM_ARRAY_TASK_ID")
)

data_file = file(paste0('/mnt/scratch/users/', user,'/bleach-paper/sbatch/run_0_8_1.sbatch'), open = "wt")
writeLines(message, con = data_file)
close(data_file)

