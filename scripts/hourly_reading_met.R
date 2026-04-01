#!/usr/bin/env Rscript
#SBATCH --job-name=reading_met
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --account=chem-cmde-2019
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=cw1781@york.ac.uk
#SBATCH --output=/mnt/scratch/users/cw1781/btt_cal_processing/logs/reading_%j.out
#SBATCH --error=/mnt/scratch/users/cw1781/btt_cal_processing/logs/logs/reading_%j.err


library(tidyverse)

reading_files = list.files("/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/met_data_formatted", 
                           pattern = "WXT_", 
                           recursive = TRUE, 
                           full.names = TRUE)

one_file = read_csv("/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/met_data_formatted/2024/12/WXT_24_12_01.dat")

#all_reading_data <- map_df(reading_files, read.csv)

check <- one_file %>% 
  mutate(
    sec = floor_date(datetime, "1 sec"),
    presAtm = X11,
    relative_humidity = X10
  ) 

all_reading_data <- map_df(
  reading_files,
  ~ read_csv(.x, col_types = cols(.default = col_character()))
) %>%
  transmute(
    datetime = ymd_hms(datetime),
    sec = floor_date(datetime, "1 sec"),
    presAtm = as.numeric(X11),
    relative_humidity = as.numeric(X10)
  )

 hourly_reading_data <- all_reading_data %>% 
  mutate(hour = floor_date(datetime, "hour")) %>% 
  group_by(hour) %>% 
  # 3. Summarise all numeric columns
  summarise(across(where(is.numeric), ~mean(.x, na.rm = TRUE)))

write_csv(hourly_reading_data, "data/data/hourly_reading.csv")



ggplot(hourly_reading_data, aes(hour, relative_humidity)) +
  geom_line() +
  theme_bw()


