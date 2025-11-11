library(DBI)
library(here)
library(dplyr)
library(tidyr)
library(lubridate)

source(here::here('functions','utils.R'))

con = connect_to_db(FALSE)

hours = tbl(con, "o3_aqd") |> 
  select(unixTime) |> 
  summarise(
    start = min(unixTime, na.rm = T),
    end = max(unixTime, na.rm = T)
  ) |> 
  pivot_longer(everything(), values_to = "unixTime") |> 
  collect() |> 
  mutate(
    unixTime = unixTime - (unixTime %% 3600),
    date = as.POSIXct(unixTime, origin = "1970-01-01 00:00", tz = "UTC")
  ) 

ts = tibble(
  unixTime_start = seq(min(hours$unixTime), max(hours$unixTime), 3600),
  unixTime_end = unixTime_start+3600
) |> 
  left_join(
    tbl(con, "th_events") |> 
      collect(),
    join_by(
      between(
        unixTime_start, date_start, date_end
      )
    )
  ) |> 
  filter(is.na(label))

dbWriteTable(con, "ts", ts, overwrite = T)

dbDisconnect(con, shutdown = T)

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
            paste0("#SBATCH --mail-user=",user,"@york.ac.uk   # Where to send mail"),
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
            paste0("apptainer exec --cwd /bleach-paper --bind /mnt/scratch/users/",user,"/bleach-paper/:/bleach-paper /mnt/longship/projects/chem-cmde-2019/eddy4r/eddy4r.york_dev Rscript /bleach-paper/scripts/0_8_1_make_ec_inputs.R $SLURM_ARRAY_TASK_ID")
)



