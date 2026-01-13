# library 
library(tidyverse)
library(waclr)


# looking at Sam's zero counts 

sam_files <- list.files(path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/sam_input_data/2023/60", 
                                 full.names = T, 
                                 pattern = ".csv",
                                 recursive = T)


sam_5hz_2023 <- sam_files %>%
  map_df(read_csv) 

sam_5hz_2023_hour <- sam_5hz_2023 %>% 
  mutate(date = as.POSIXct(date, format = "%Y-%m-%d %H:%M:%OS", tz = "UTC")) %>% 
  mutate(hour = hour(date)) %>% 
  group_by(hour) %>% 
  summarise()

sam_5hz_2023_hour %>% 
  pivot_longer(cols = c(ch1_hz, ch2_hz), 
               names_to = "channel", values_to = "counts") %>% 
  ggplot(aes(hour, counts, color = channel)) +
  geom_line(alpha = 0.5) +
  theme_bw() +
  facet_wrap(~channel) +
  labs(x = "Date", y = "Zero counts")




