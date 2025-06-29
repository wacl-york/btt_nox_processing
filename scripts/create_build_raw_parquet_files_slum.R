config = ini::read.ini("/btt_nox_processing/config.ini")

source("/btt_nox_processing/functions/get_raw_dirs.R")

dirs = get_raw_dirs()

message = c("#!/bin/bash",
            "#SBATCH --job-name='btt_build_parquet'",
            "#SBATCH --time=00:05:00",
            "#SBATCH --mem=10G",
            "#SBATCH --account=ncas_obs",
            "#SBATCH --partition=standard",
            "#SBATCH --qos=standard",
            "#SBATCH -o /gws/pw/j07/ncas_obs_vol1/bttao/logs/%x/%j/%a/%j.out",
            "#SBATCH -e /gws/pw/j07/ncas_obs_vol1/bttao/logs/%x/%j/%a/%j.err",
            paste0("#SBATCH --array=0-", nrow(dirs)-1),
            "",
            "singularity exec /gws/pw/j07/ncas_obs_vol1/bttao/software/btt_nox_processing_dev.sif Rscript /btt_nox_processing/scripts/build_raw_parquet_files_array_task.R $SLURM_ARRAY_TASK_ID"
)

writeLines(message, con = file.path(config$paths$software, "build_raw_parquet_files.slurm"))