library(tidyverse)
library(plotly)



bound_fluxes = read_csv("/mnt/scratch/users/cw1781/btt_flux_analysis/flux_analysis/data/flux_data/flux_all.csv")


bound_fluxes %>% 
  filter(date <= "2023-09-01 00:00:00") %>% 
  ggplot(aes(date, presAtm)) +
  geom_line() +
  theme_bw()
  
# compare to the modeled pressure 

ERA5_pressure_corrected = read_csv("/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/modelled_data/pressure_corrected.csv")

pressure_fluxes = bound_fluxes %>% 
  select(c(hour, presAtm)) %>% 
  rename("valid_time" = hour)

both = left_join(pressure_fluxes, ERA5_pressure_corrected, by = "valid_time") 

both %>% 
  pivot_longer(cols = c(presAtm, pres_190m_pa), 
               values_to = "pressure", names_to = "type") %>% 
  filter(valid_time < "2022-01-01 00:00:00") %>% 
  ggplot(aes(valid_time, pressure, color = type)) +
  geom_line() +
  theme_bw()



ggplotly()

