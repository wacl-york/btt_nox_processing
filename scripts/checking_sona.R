library(tidyverse)
library(plotly)

# checking the hourly sona data 


sona_2020 = read_csv("data/data/2020.csv")

check_2 = sona_2020 %>% 
  filter(datetime == "2020-10-03 09:00:00") 

sona_2020_edit = sona_2020 %>% 
  filter(h2o_ppthou < 10) %>% 
  rename("hour" = datetime)



ggplot(aes(datetime, h2o_ppthou))+
  geom_line() +
  theme_bw()


ggplotly()

file_path = "/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/sona_data/2020/Sonic_Licor_BT_20201003_0900.sona"

time_string <- str_extract(basename(file_path), "\\d{8}_\\d{4}")
start_time <- ymd_hm(time_string, tz = "UTC")

# 3. Read the data (since it has headers, read_csv works perfectly)
check_one <- read_csv(file_path)

# 4. Create the 20Hz clock
# 1/20Hz = 0.05 seconds per row
check_one <- check_one %>%
  mutate(
    row_idx = row_number() - 1,
    datetime = start_time + (row_idx * 0.05)
  )

ggplot(check_one, aes(datetime, h2o_ppthou))+
  geom_line()

# hourly files 

hourly_sona = list.files("/mnt/scratch/users/cw1781/btt_cal_processing/btt_nox_processing/data/data/", 
                         pattern = "sona_hourly", 
                         full.names = T)

hourly_sona_data = map_df(hourly_sona, read_csv)

hourly_sona_data %>% 
  filter(h2o_ppthou > -10) %>% 
  filter(h2o_ppthou < 100) %>% 
 # filter(co2_dry_ppm < 10000) %>% 
  filter(co2_ppm > 0) %>% 
ggplot(aes(datetime, co2_ppm))+
  geom_line()

ggplotly()


# compare this to Sam's input files? 

bound_fluxes = read_csv("/mnt/scratch/users/cw1781/btt_flux_analysis/flux_analysis/data/flux_data/flux_all.csv")


bound_fluxes_2020 = bound_fluxes %>% 
  filter(hour < "2021-01-01 00:00:00")

both = left_join(sona_2020_edit, bound_fluxes_2020, by = "hour")

both %>% 
  mutate(
    # 1. Convert Percent to Wet Molar Ratio (mol/mol)
    rtio_wet_sona = h2o_ppthou / 100, 
    
    # 2. Convert Wet Molar Ratio to Dry Molar Ratio
    # This is the "Dry Air" correction: chi_dry = chi_wet / (1 - chi_wet)
    sona_rtioMoleDryH2o = rtio_wet_sona / (1 - rtio_wet_sona)
  ) %>% 
  pivot_longer(cols = c(sona_rtioMoleDryH2o, rtioMoleDryH2o), 
               names_to = "type", values_to = "water") %>% 
  ggplot(aes(hour, water, color = type))+
  geom_line() +
  theme_bw()


