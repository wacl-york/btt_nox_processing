#!/usr/bin/env Rscript
#SBATCH --job-name=run_ec # Job name
#SBATCH --ntasks=1                      # Number of MPI tasks to request
#SBATCH --cpus-per-task=1               # Number of CPU cores per MPI task
#SBATCH --mem=10G                      # Total memory to request
#SBATCH --time=0-00:30:00               # Time limit (DD-HH:MM:SS)
#SBATCH --account=chem-cmde-2019        # Project account to use
#SBATCH --mail-type=END,FAIL            # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=cw1781@york.ac.uk   # Where to send mail
#SBATCH --output=/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_logs/logs_2025/%x_%j_%a.log       # Standard output log
#SBATCH --error=/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_logs/logs_2025/%x_%j_%a.err        # Standard error log
#SBATCH --array=0-7063          # Array range
# purge any existing modules
system('module purge')
# Commands to run

# Filter the input files based on the array ID
files = system('find /mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec/in/2025 -type f -name *.csv', intern = T) |> sort()
hostRoot = '/mnt/scratch/projects/chem-cmde-2019/btt_processing'
slurm_array_task_id = as.numeric(Sys.getenv('SLURM_ARRAY_TASK_ID'))+1
FILE = files[slurm_array_task_id]
FILE = stringr::str_remove(FILE, hostRoot)
system(paste0('set -e && module load Apptainer/latest && apptainer exec --env FILE_SELECT=',FILE,' --bind /mnt/scratch/projects/chem-cmde-2019/btt_processing/processing:/processing/,/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/ec/:/scripts/ /mnt/longship/projects/chem-cmde-2019/eddy4r/eddy4r.york_dev Rscript /scripts/1_0_1_eddy4r_config_2025.R'))
