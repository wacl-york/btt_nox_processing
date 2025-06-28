# pass SLURM_ARRAY_TASK_ID as the first argument  
args = commandArgs(trailingOnly = T)
SLURM_ARRAY_TASK_ID = as.numeric(args[1])+1

config = ini::read.ini("/btt_nox_processing/config.ini")

dirs = list.dirs(config$paths$raw_data) 
dirs = dplyr::tibble(dir = stringr::str_remove(dirs, config$paths$raw_data))
dirs = dirs |> 
  dplyr::filter(stringr::str_detect(dir, "/")) |> 
  tidyr::separate_wider_delim(cols = dir, delim = "/", names = c("yyy","type", "year", "month"), too_few = "align_start") |> 
  dplyr::select(-yyy) |> 
  dplyr::filter(
    !is.na(month),
    type %in% c("five_hz", "params")
  )

# 1 == ignore, 2 == type, 3 == yr, 4 == month
arguments = stringr::str_split(dirs, "/")

system(
  paste(
  "Rscript",
  "/btt_nox_processing/scripts/build_raw_parquet_files.R",
  dirs$type[[SLURM_ARRAY_TASK_ID]],
  dirs$year[[SLURM_ARRAY_TASK_ID]],
  dirs$month[[SLURM_ARRAY_TASK_ID]]
  )
)