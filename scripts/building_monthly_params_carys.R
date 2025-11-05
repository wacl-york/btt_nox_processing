library(dplyr)
library(arrow)
library(tidyr)
library(lubridate)

# Root folder where year folders live
data_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/params"

data_out <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_2"

# Years you want to process
years <- 2025:2025  # adjust as needed
months <- sprintf("%02d", 10:10)  # "01" to "12"

# Loop over years and months
for (yr in years) {
  for (mo in months) {
    cat("Processing", yr, mo, "\n")
    
    month_folder <- file.path(data_root, as.character(yr), mo)
    if (!dir.exists(month_folder)) next
    
    # List all param files in the month folder (ignore other files)
    param_files <- list.files(month_folder, pattern = "param", full.names = TRUE)
    if (length(param_files) == 0) next
    
    # Read all files in the month and combine
    month_data <- lapply(param_files, function(f) {
      read.csv(f) %>%
        tibble::as_tibble() %>%
        tibble::repair_names() %>%
        mutate(across(everything(), as.numeric))
    }) %>% bind_rows()
    
    # --- NEW: create a year-specific output folder ---
    year_out_dir <- file.path(data_out, as.character(yr))
    dir.create(year_out_dir, showWarnings = FALSE, recursive = TRUE)
    
    # Save combined parquet for the month
    out_file <- file.path(year_out_dir, paste0("param_", yr, "_", mo, ".parquet"))
    write_parquet(month_data, out_file)
    
    cat("Saved:", out_file, "\n")
  }
}
