# pass SLURM_ARRAY_TASK_ID as the first argument  
args = commandArgs(trailingOnly = T)
SLURM_ARRAY_TASK_ID = as.numeric(args[1])

config = ini::read.ini("/btt_nox_processing/config.ini")

dirs = list.dirs(config$paths$raw_data) 
dirs = dirs[nchar(dirs) == max(nchar(dirs))]
dirs = stringr::str_remove(dirs, config$paths$raw_data) 

message = c("#!/bin/bash",
            "#SBATCH --job-name='btt_build_parquet'",
            "#SBATCH --time=00:05:00",
            "#SBATCH --mem=1G",
            "#SBATCH --account=ncas_obs",
            "#SBATCH --partition=standard",
            "#SBATCH --qos=standard",
            "#SBATCH -o %j.out",
            "#SBATCH -e %j.err",
            paste0("#SBATCH --array=0-", length(dirs)-1),
            "",
            "singularity exec btt_nox_processing_dev.sif Rscript /btt_nox_processing/scripts/build_raw_parquet_files_array_task.R $SLURM_ARRAY_TASK_ID",
)

writeLines(message, con = file.path(config$paths$software, "build_raw_parquet_files.slurm"))