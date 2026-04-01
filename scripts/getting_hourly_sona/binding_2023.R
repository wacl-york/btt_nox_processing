# Run this once the Slurm Array is 'COMPLETED'
library(tidyverse)

final_2023 <- list.files("/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/data/data/temp_sona/2023", 
                         full.names = TRUE) %>%
  map_df(read_csv) %>%
  arrange(datetime) # This puts them in the correct time order!

write_csv(final_2023, "/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/data/data/sona_hourly_2023.csv")
