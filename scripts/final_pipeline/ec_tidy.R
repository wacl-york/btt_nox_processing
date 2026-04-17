# path is to directory to run "find" on that will recursively list all files.
list_ec_outputs = function(path){
  tibble::tibble(filePath = system(paste("find", path, "-type f"), intern = T)) |> 
    dplyr::mutate(
      fileName = basename(filePath),
      fileType = dplyr::case_when(
        stringr::str_detect(fileName, "_max") ~ "max",
        stringr::str_detect(fileName, "_isca") ~ "isca",
        stringr::str_detect(fileName, "_data") ~ "data",
        stringr::str_detect(fileName, "_conv") ~ "conv",
        stringr::str_detect(fileName, "_foot") ~ "foot",
        stringr::str_detect(fileName, "_base") ~ "base",
        stringr::str_detect(fileName, "_ACF") ~ "ACF",
        stringr::str_detect(fileName, "_diff") ~ "diff",
        stringr::str_detect(fileName, "_spec") ~ "spec",
        stringr::str_detect(fileName, "_lod.csv") ~ "lod",
        stringr::str_detect(fileName, "_lod_wrap_lag.csv") ~ "lod_wrap_lag",
        stringr::str_detect(fileName, "_lagTimes") ~ "lagTimes",
        stringr::str_detect(fileName, "_logfile") ~ "logfile",
        stringr::str_detect(fileName, "_itc") ~ "itc",
        stringr::str_detect(fileName, "_error") ~ "error",
        stringr::str_detect(fileName, "_stna") ~ "stna",
        stringr::str_detect(fileName, "_sd") ~ "sd",
        stringr::str_detect(fileName, "_para") ~ "para",
        stringr::str_detect(fileName, "_mtrxRot01") ~ "mtrxRot01",
        stringr::str_detect(fileName, "_min") ~ "min",
        stringr::str_detect(fileName, "_corr") ~ "corr",
        stringr::str_detect(fileName, "_mean") ~ "mean"
      ),
      fileExtension = dplyr::case_when(
        stringr::str_detect(fileName, ".csv.gz") ~ ".csv.gz",
        stringr::str_detect(fileName, ".csv") ~ ".csv",
        stringr::str_detect(fileName, ".RDS") ~ ".RDS",
        stringr::str_detect(fileName, ".txt") ~ ".txt"
      )
    )
}


# ecOutputs is the list of files created by list_ec_outputs
# analysisText is the text from the filename derived from the analysis that is not the date component
#   for .csvs the format is paste0(analysisText,"_%Y%m%d_%H_",fileType,fileExtension)
#   for .csv.gz the format is paste0("%Y%m%d_%H_",analysisText,"_%Y%m%d_%H_",fileType,fileExtension)
# outputDirectory is the directory in which to save the bound outputs that will contain the yr/mnth folders

collate_ec_outputs = function(
    ecOutputs,
    outputDirectory,
    analysisText
){
  
  ecOutputs <- ecOutputs |>
    dplyr::mutate(
      fileDate = dplyr::coalesce(
        # compressed files: 20250101_04_...
        stringr::str_extract(fileName, "\\d{8}_\\d{2}") |>
          as.POSIXct(format = "%Y%m%d_%H", tz = "UTC"),
        # standard files: 25_01_01_040000
        stringr::str_extract(fileName, "\\d{2}_\\d{2}_\\d{2}_\\d{6}") |>
          as.POSIXct(format = "%y_%m_%d_%H%M%S", tz = "UTC")
      ),
      yr   = lubridate::year(fileDate),
      mnth = stringr::str_pad(
        lubridate::month(fileDate),
        width = 2,
        pad = "0"
      ),
      outDir = file.path(
        outputDirectory,
        fileType,
        paste0("year=", yr),
        paste0("month=", mnth)
      ),
      
      outPath = file.path(
        outDir,
        paste0(yr, "_", mnth, "_", fileType, ".parquet")
      )
    ) |>
    # sort chronologically
    dplyr::arrange(fileDate) |>
    # group into parquet-level chunks
    dplyr::nest_by(outPath, outDir, fileType) |>
    dplyr::ungroup() |>
    dplyr::select(outDir, outPath, data, fileType)
}

ec_tidy_array_task = function(ecOutputsCollated, SLURM_ARRAY_TASK_ID, types){
  
  i = as.numeric(SLURM_ARRAY_TASK_ID)+1
  
  data = ecOutputsCollated$data[[i]]
  outDir = ecOutputsCollated$outDir[i]
  outPath = ecOutputsCollated$outPath[i]
  fileType = ecOutputsCollated$fileType[i]
  
  
  print(paste0("[info] fileType: ", fileType))
  print(paste0("[info] outPath: ", outPath))
  
  # if this a type we want to bind, do that first
  if(fileType %in% types){
    
    dat = purrr::map_df(data$filePath, read.csv)
    
    if(fileType == "foot"){
      
      dat = dat |> 
        dplyr::mutate(value = round(value, 5)) |> 
        dplyr::filter(value != 0)
    }
    
    if(!dir.exists(outDir)){
      dir.create(outDir, recursive = T)
    }
    
    arrow::write_parquet(dat, outPath)
  }
  
  # Remove the unbound files from disk
 # file.remove(data$filePath)
  
}

get_job_status = function(jobid){
  system(paste0("sacct -nXP -j ", jobid," -o state%20"), intern = T) |> 
    unique()
}

is_job_complete = function(jobid){
  jobStatus = get_job_status(jobid)
  
  sum(jobStatus %in% c("RUNNING", "PENDING")) == 0
  
}


read_ec_log = function(path){
  lines = readLines(path)
  lines = lines[grep("\\[\\s*(info|error|warn)\\s*\\]",x = lines)]
  
  if(length(lines) > 0){
    date = lines |> 
      stringr::word(sep = " ")
    
    time = lines |> 
      stringr::word(sep = " ", 2)
    
    tibble::tibble(
      date = as.POSIXct(paste(date, time), format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
      message = lines
    )
  }else{
    return(NULL)
  }
}

submit_ec_job = function(
    runID,
    user,
    account_code, 
    dataPath,
    bindPath,
    containerPath,
    scriptPath,
    tempDir = "tmp",
    tempFile = "tempSbatch"){
  
  files = system(paste0('find ',dataPath,' -type f -name *.csv'), intern = T) |>
    sort()
  
  if(!dir.exists(here::here(tempDir))){
    dir.create(here::here(tempDir))
  }
  
  tempSbatch = here::here(tempDir,tempFile)
  
  glue::glue(
    "
  #!/usr/bin/env Rscript
  #SBATCH --job-name={runID} # Job name
  #SBATCH --ntasks=1
  #SBATCH --cpus-per-task=1
  #SBATCH --mem=10G         
  #SBATCH --time=0-00:30:00     
  #SBATCH --account={account_code}
  #SBATCH --mail-type=END,FAIL  
  #SBATCH --mail-user={user}@york.ac.uk
  #SBATCH --output={logs_path(runID,'%x_%j_%a.log')}
  #SBATCH --error={logs_path(runID,'%x_%j_%a.err')}
  #SBATCH --array=0-{length(files)-1}
  
  # Filter the input files based on the array ID
  files = system('find {dataPath} -type f -name *.csv', intern = T) |> sort()
  hostRoot = '/mnt/scratch/users/{user}'
  slurm_array_task_id = as.numeric(Sys.getenv('SLURM_ARRAY_TASK_ID'))+1
  FILE = files[slurm_array_task_id]
  FILE = stringr::str_remove(FILE, hostRoot)
  system(paste0('set -e && module load Apptainer/latest && apptainer exec --env FILE_SELECT=',FILE,' --bind {bindPath}:/bleach-paper {containerPath} Rscript {scriptPath}'))
  "
  ) |> 
    writeLines(tempSbatch)
  
  # Submit Job --------------------------------------------------------------
  
  resp = system(glue::glue("sbatch {tempSbatch}"), intern = T)
  
  # Clean up ----------------------------------------------------------------
  
  unlink(here::here(tempDir), recursive = T)
  
  return(resp)
}

submit_ec_tidy_job = function(
    runID, 
    user,
    account_code,
    tempEcCollatedOutputs,
    types
){
  
  job_name = glue::glue("tidy_{runID}")
  
  ecOutputsCollated = readRDS(tempEcCollatedOutputs)
  
  typesString = paste0("c('",paste(types, collapse = "','"),"')")
  
  glue::glue(
    "
  #!/usr/bin/env Rscript
  #SBATCH --job-name={job_name}
  #SBATCH --ntasks=1
  #SBATCH --cpus-per-task=1 
  #SBATCH --mem=32G
  #SBATCH --time=0-00:20:00 
  #SBATCH --account={account_code}
  #SBATCH --mail-type=END,FAIL
  #SBATCH --mail-user={user}@york.ac.uk
  #SBATCH --output={logs_path({job_name},'%x_%j_%a.log')}
  #SBATCH --error={logs_path({job_name},'%x_%j_%a.err')}
  #SBATCH --array=0-{nrow(ecOutputsCollated)-1}
  
  system(paste0('set -e'))
  
  library(dplyr)
  
  source('/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/final_pipeline/utils.R')
  source('/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/scripts/final_pipeline/ec_tidy.R')
  
  ecOutputsCollated = readRDS('{tempEcCollatedOutputs}')
  
  ec_tidy_array_task(ecOutputsCollated, Sys.getenv('SLURM_ARRAY_TASK_ID'), {typesString})
  "
  ) |> 
    writeLines(tempSbatch)
  
  # Submit Job  -------------------------------------------------------------
  
  resp = system(glue::glue("sbatch {tempSbatch}"), intern = T)
  
  return(resp)
  
}

ec_parquet_load_list = function(boundOutputPath, runID){
  
  tibble(
    dirPath = list.dirs(boundOutputPath, recursive = F),
    fileType = list.dirs(boundOutputPath, recursive = F, full.names = F)
  ) |> 
    mutate(
      loadPath = file.path(dirPath,"*","*","*.parquet"),
      tableName = paste0("ec_", runID, "_", fileType)) |> 
    select(loadPath, tableName)
  
}
