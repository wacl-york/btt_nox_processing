# Reads the raw .csv data for 5hz or params, per month and writes a parquet file
# works on 1 month of data at a time
# 
# Designed to be called via an array job to build everything at once
#
# uses three trailing arguments corresponding to the type, year and month folders
# that this job is being pointed at

library(ini)
library(here)
library(dplyr)
library(purrr)
library(arrow)

source("/btt_nox_processing/functions/tidy_raw_csvs.R")

config = read.ini("/btt_nox_processing/config.ini")

args = commandArgs(trailingOnly = TRUE)

type = args[1] # "five_hz"
yr = args[2] # "2021"
mnth = args[3] # "02"

files = list.files(
  path = file.path(config$paths$raw_data, type, yr, mnth), 
  full.names = T)

if(length(files) != 0){
  
  arrowRead = map(files, safely(\(x) arrow::read_csv_arrow(x), NULL))
  
  arrowReadData = arrowRead |> 
    map(pluck("result"))
  
  arrowReadErrors = arrowRead |> 
    map(pluck("error"))
  
  whereError = lapply(arrowReadErrors, \(x) !is.null(x)) |> 
    unlist() |> 
    which()
  
  # if we got read errors via arrow::read_csv_arrow, try these using read.csv
  if(length(whereError) > 0){ 
    csvRead = map(files[whereError], safely(\(x) tibble(read.csv(x)), NULL))
    
    csvReadData = csvRead |> 
      map(pluck("result"))
    
    csvReadErrors = csvRead |> 
      map(pluck("error"))
    
    for(i in 1:length(whereError)){
      arrowReadData[[whereError[i]]] = csvReadData[[i]]
      arrowReadErrors[[whereError[i]]] = csvReadErrors[[i]]
    }
    
  }
  
  dataFileName = paste0("NOx_5Hz_", basename(yr),"_", mnth, ".parquet")
  dataDirOut = file.path(config$paths$raw_parquet,"data", type, yr)
  
  if(!dir.exists(dataDirOut)){
    dir.create(dataDirOut, recursive = TRUE)
  }
  
  data = arrowReadData |>
    tidy_raw_csvs(type)
  
  write_dataset(data,file.path(dataDirOut, dataFileName), format = "parquet")
  
  errors = purrr::discard(arrowReadErrors, \(x) is.null(x))
  
  if(length(errors) > 0){
    errorFileName = paste0("NOx_5Hz_error_", basename(yr),"_", mnth, ".RDS")
    
    errorDirOut = file.path(config$paths$raw_parquet,"error", type)
    
    if(!dir.exists(errorDirOut)){
      dir.create(errorDirOut, recursive = TRUE)
    }
    
    saveRDS(errors, file.path(errorDirOut, errorFileName), format = "parquet")
    
  }
  
}else{
  print("no files in folder")
}
