logs_path = function(...){
  
  # here::here(readLines(here::here("data_path.txt"), n = 1), "logs", ...)
  here::here("logs", ...)
}

data_path = function(...){
  
  # here::here(readLines(here::here("data_path.txt"), n = 1), "data", ...)
  here::here("data", ...)
}

connect_to_db = function(read_only = TRUE){
  
  con = DBI::dbConnect(
    drv = duckdb::duckdb(),
    dbdir = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec_2/duckdb/ec_BT.duckdb",
    read_only = read_only
  )
  
  con
  
}