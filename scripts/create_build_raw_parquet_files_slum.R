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

message = c("#!/bin/bash",
            "#SBATCH --job-name='btt_build_parquet'",
            "#SBATCH --time=00:05:00",
            "#SBATCH --mem=1G",
            "#SBATCH --account=ncas_obs",
            "#SBATCH --partition=standard",
            "#SBATCH --qos=standard",
            "#SBATCH -o %j.out",
            "#SBATCH -e %j.err",
            paste0("#SBATCH --array=0-", nrow(dirs)-1),
            "",
            "singularity exec btt_nox_processing_dev.sif Rscript /btt_nox_processing/scripts/build_raw_parquet_files_array_task.R $SLURM_ARRAY_TASK_ID"
)

writeLines(message, con = file.path(config$paths$software, "build_raw_parquet_files.slurm"))