# pass SLURM_ARRAY_TASK_ID as the first argument  
args = commandArgs(trailingOnly = T)
SLURM_ARRAY_TASK_ID = as.numeric(args[1])+1

config = ini::read.ini("/btt_nox_processing/config")

dirs = list.dirs(config$paths$raw_data) 
dirs = dirs[nchar(dirs) == max(nchar(dirs))]
dirs = stringr::str_remove(dirs, config$paths$raw_data) 


# 1 == ignore, 2 == type, 3 == yr, 4 == month
arguments = stringr::str_split(dirs, "/")

system(
  paste(
  "Rscript",
  "/btt_nox_processing/scripts/build_raw_parquet_files.R",
  arguments[[SLURM_ARRAY_TASK_ID]][2],
  arguments[[SLURM_ARRAY_TASK_ID]][3],
  arguments[[SLURM_ARRAY_TASK_ID]][4]
  )
)