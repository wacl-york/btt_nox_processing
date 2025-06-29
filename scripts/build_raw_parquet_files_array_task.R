# pass SLURM_ARRAY_TASK_ID as the first argument  
args = commandArgs(trailingOnly = T)
SLURM_ARRAY_TASK_ID = as.numeric(args[1])+1

config = ini::read.ini("/btt_nox_processing/config.ini")

source("/btt_nox_processing/functions/get_raw_dirs.R")

dirs = get_raw_dirs()

system(
  paste(
  "Rscript",
  "/btt_nox_processing/scripts/build_raw_parquet_files.R",
  dirs$type[[SLURM_ARRAY_TASK_ID]],
  dirs$year[[SLURM_ARRAY_TASK_ID]],
  dirs$month[[SLURM_ARRAY_TASK_ID]]
  )
)