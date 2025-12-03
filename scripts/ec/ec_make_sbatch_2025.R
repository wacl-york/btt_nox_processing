

dataPath = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec/in/2025"

files = system(paste0('find ',dataPath,' -type f -name *.csv'), intern = T) |>
  sort()

user = system("echo $USER", intern = T)

outputFile = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_logs/logs_2025/%x_%j_%a.log"
errFile = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_logs/logs_2025/%x_%j_%a.err"

containerPath = "/mnt/longship/projects/chem-cmde-2019/eddy4r/eddy4r.york_dev"

message = c("#!/usr/bin/env Rscript",
            "#SBATCH --job-name=run_ec # Job name",
            "#SBATCH --ntasks=1                      # Number of MPI tasks to request",
            "#SBATCH --cpus-per-task=1               # Number of CPU cores per MPI task",
            "#SBATCH --mem=10G                      # Total memory to request",
            "#SBATCH --time=0-00:30:00               # Time limit (DD-HH:MM:SS)",
            "#SBATCH --account=chem-cmde-2019        # Project account to use",
            "#SBATCH --mail-type=END,FAIL            # Mail events (NONE, BEGIN, END, FAIL, ALL)",
            paste0("#SBATCH --mail-user=",user,"@york.ac.uk   # Where to send mail"),
            paste0("#SBATCH --output=",outputFile,"       # Standard output log"),
            paste0("#SBATCH --error=",errFile,"        # Standard error log"),
            paste0("#SBATCH --array=0-",length(files)-1,"          # Array range"),
            "# purge any existing modules",
            "system('module purge')",
            "# Commands to run",
            "",
            "# Filter the input files based on the array ID",
            paste0("files = system('find ",dataPath," -type f -name *.csv', intern = T) |> sort()"),
            paste0("hostRoot = '/mnt/scratch/projects/chem-cmde-2019/btt_processing'"),
            "slurm_array_task_id = as.numeric(Sys.getenv('SLURM_ARRAY_TASK_ID'))+1",
            "FILE = files[slurm_array_task_id]",
            "FILE = stringr::str_remove(FILE, hostRoot)",
            paste0(
              "system(paste0('",
              "set -e && ",
              "module load Apptainer/latest && ", # load apptainer
              "apptainer exec --env FILE_SELECT=',FILE,' --bind /mnt/scratch/projects/chem-cmde-2019/btt_processing/processing:/processing/", 
              "/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/ec/:/scripts/ ",
              containerPath, # path to container
              " Rscript /scripts/1_0_1_eddy4r_config.R'))") # exec command
)

data_file = file(paste0('/mnt/scratch/users/',user,'/btt_cal_processing/btt_nox_processing/sbatch/run_1_0_1_2025.sbatch'), open = "wt")
writeLines(message, con = data_file)
close(data_file)