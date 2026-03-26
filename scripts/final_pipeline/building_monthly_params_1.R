# library
library(dplyr)
library(arrow)
library(tidyr)
library(lubridate)
library(waclr)

# data paths
data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/params"
data_out  <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_edit"

# change depending on what you want to process
years  <- 2026:2026
months <- sprintf("%02d", 1:2)

# Helper: get last param file in a folder
get_last_param_file <- function(folder) {
  files <- list.files(folder, pattern = "param", full.names = TRUE)
  if (length(files) == 0) return(NULL)
  files[order(files)][length(files)]
}

for (yr in years) {
  for (mo in months) {
    cat("Processing", yr, mo, "\n")
    
    # ---- month boundaries ----
    month_start <- ymd_hms(sprintf("%d-%s-01 00:00:00", yr, mo), tz = "UTC")
    month_end   <- month_start %m+% months(1)
    
    # ---- current month files ----
    current_folder <- file.path(data_root, yr, mo)
    current_files <- if (dir.exists(current_folder)) {
      list.files(current_folder, pattern = "param", full.names = TRUE)
    } else {
      character(0)
    }
    
    # ---- previous month: last file only ----
    prev_date <- month_start %m-% months(1)
    prev_folder <- file.path(
      data_root,
      year(prev_date),
      sprintf("%02d", month(prev_date))
    )
    
    prev_file <- if (dir.exists(prev_folder)) {
      get_last_param_file(prev_folder)
    } else {
      NULL
    }
    
    # ---- combine file list ----
    param_files <- c(prev_file, current_files)
    param_files <- param_files[!is.na(param_files)]
    
    if (length(param_files) == 0) next
    
    # ---- read, filter, combine ----
    month_data <- lapply(param_files, function(f) {
      read.csv(f) %>%
        tibble::as_tibble() %>%
        tibble::repair_names() %>%
        mutate(
          time = parse_excel_date(TheTime, tz = "UTC")
        )
    }) %>%
      bind_rows() %>%
      filter(time >= month_start, time < month_end) %>%
      arrange(time) %>%
      select(-time) %>%                     # drop time if not needed
      mutate(across(everything(), as.numeric))
    
    # ---- output ----
    year_out_dir <- file.path(data_out, as.character(yr))
    dir.create(year_out_dir, showWarnings = FALSE, recursive = TRUE)
    
    out_file <- file.path(year_out_dir, paste0("param_", yr, "_", mo, ".parquet"))
    write_parquet(month_data, out_file)
    
    cat("Saved:", out_file, "\n")
  }
}




# checking files 

 check_parquet_file2 <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_edit/2025/param_2025_12.parquet"
 
 check_parquet<- open_dataset(check_parquet_file2, format = "parquet") %>%
   collect() %>% 
   mutate(datetime = parse_excel_date(TheTime, tz = "UTC"))
 
 
 head(check_parquet$datetime)
 tail(check_parquet$datetime)
 
 

