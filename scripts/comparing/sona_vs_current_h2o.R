library(tidyverse)
library(purrr)
library(waclr)



#  testing to see if the Sona data is the same as Sam's input data 


sona_list <- list.files(path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/sona_data/2023", 
                        pattern = "_20230823", 
                        recursive = T, 
                        full.names = T)

sona_data <- map_df(sona_list, function(file) {
  # Extract YYYYmmdd_HHMM from filename (adjust regex if needed)
  file_ts <- str_extract(basename(file), "\\d{8}_\\d{4}")
  start_time <- as.POSIXct(file_ts, format = "%Y%m%d_%H%M", tz = "UTC")
  
  df <- read.csv(file)
  
  # Since it's 20Hz, create a precise timestamp for every row
  # (1/20 = 0.05 seconds)
  df %>%
    mutate(timestamp = start_time + seq(0, by = 0.05, length.out = n()))
})

sona_1min <- sona_data %>%
  mutate(time_1min = floor_date(timestamp, "1 minute")) %>%
  group_by(time_1min) %>%
  summarise(h2o_sona = mean(h2o_ppthou, na.rm = TRUE)) %>% 
  mutate(h2o_sona = h2o_sona/1000)

my_files_input <- list.files(path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/ec/in/2023/08/", 
                             pattern = "23_08_23", 
                             recursive = T, 
                             full.names = T)

my_input_data <- map_df(my_files_input, read_csv) %>% 
  mutate(date = as.POSIXct(unixTime, origin = "1970-01-01", tz = "UTC"))

my_input_data1min <- my_input_data %>% 
  mutate(time_1min = floor_date(date, "1 minute")) %>%
  group_by(time_1min) %>%
  summarise(h2o_me = mean(rtioMoleDryH2o, na.rm = TRUE))

join_both = left_join(sona_1min, my_input_data1min, by = "time_1min")

join_both %>% 
  pivot_longer(cols = c(h2o_me, h2o_sona), 
               names_to = "type", values_to = "water") %>% 
  ggplot(aes(time_1min, water, color = type)) +
  geom_line()





