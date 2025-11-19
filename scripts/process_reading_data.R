


met_root <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/reading_met_data"


reading_files <- list.files(met_root, pattern = "WXT", full.names = TRUE, recursive = TRUE)


example1 <- read_csv("/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/reading_met_data/2024/10112024/WXT24314.dat", 
                    col_names = FALSE)

example2 <- read_csv("/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/reading_met_data/2024/10122024/WXT24344.dat", 
                     col_names = FALSE)


example3 <- read_csv("/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/reading_met_data/2025/04022025/WXT25034.dat", 
                     col_names = FALSE)


#  reformat and save the reading data 


library(readr)
library(dplyr)
library(fs)

preprocess_wxt_year <- function(met_root, year, out_root) {
  
  year_path <- file.path(met_root, year)
  if (!dir_exists(year_path)) stop("Year directory does not exist: ", year_path)
  
  all_files <- dir_ls(year_path, recurse = TRUE, regexp = "WXT", type = "file")
  message("Found ", length(all_files), " WXT files.")
  
  for (file in all_files) {
    message("\n----------------------------------")
    message("Processing: ", file)
    
    df <- try(read_csv(
      file,
      col_names = FALSE,
      na = c("", "NA"),
      show_col_types = FALSE
    ), silent = TRUE)
    
    if (inherits(df, "try-error") || nrow(df) == 0) {
      warning("  ⚠ Could not read file, skipping.")
      next
    }
    
    if (ncol(df) < 6) {
      warning("  ⚠ File has too few columns (<6), skipping.")
      next
    }
    
    # Extract date from first row
    yy <- as.integer(df$X1[1])
    mm <- as.integer(df$X2[1])
    dd <- as.integer(df$X3[1])
    
    if (any(is.na(c(yy, mm, dd)))) {
      warning("  ⚠ Invalid date fields in file, skipping.")
      next
    }
    
    # Create datetime from sec_counter
    sec_counter <- as.numeric(df$X6)
    sec_since_start <- sec_counter - sec_counter[1]
    date_midnight <- as.POSIXct(sprintf("%04d-%02d-%02d 00:00:00", yy, mm, dd), tz = "UTC")
    datetime <- date_midnight + sec_since_start
    
    # Add datetime and sec_since_start to the existing df, keep all X7, X8, ...
    df_processed <- df %>%
      mutate(
        datetime = datetime,
        sec_since_start = sec_since_start
      )
    
    # Build correct folder and filename in output root
    month_folder <- sprintf("%02d", mm)
    target_folder <- file.path(out_root, as.character(yy), month_folder)
    dir_create(target_folder, recurse = TRUE)
    
    # Build filename: WXT_yy_mm_dd.dat
    new_filename <- sprintf("WXT_%02d_%02d_%02d.dat", yy %% 100, mm, dd)
    target_path <- file.path(target_folder, new_filename)
    
    # Save
    write_csv(df_processed, target_path)
    message("✔ Saved to: ", target_path)
  }
  
  message("\nDONE.")  # <- Now correctly outside the for loop
}


preprocess_wxt_year("/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/reading_met_data", 2024, 
                    "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/met_data_formatted")



check <- read_csv("/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/met_data_formatted/2025/04/WXT_25_04_03.dat")


