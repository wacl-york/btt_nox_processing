# library

library(tidyverse)
library(plotly)

# checking the modelled pressure data 


ERA5_pressure = read_csv("/mnt/scratch/projects/chem-cmde-2019/btt_processing/raw_data/ERA5/reanalysis-era5-single-levels-timeseries-sfcf2tgx155.csv")


head(ERA5_pressure)

ggplot(ERA5_pressure, aes(valid_time, sp))+
  geom_line()


# 1. Define Constants
g <- 9.80665
Rd <- 287.05
dz <- 190  # Height of BT Tower

# 2. Perform the "Lift" 
# Note: Ensure t2m is in Kelvin (approx 273-300) and sp is in Pascals (approx 101325)

ERA5_pressure_corrected = ERA5_pressure %>% 
  mutate(pres_190m_pa = sp * exp((-g * dz) / (Rd * t2m))) %>% 
  mutate(
    temp_c = t2m - 273.15,
    dew_c  = d2m - 273.15,
    # Saturation vapor pressure (es) and actual vapor pressure (e)
    es = 6.112 * exp((17.67 * temp_c) / (temp_c + 243.5)),
    e  = 6.112 * exp((17.67 * dew_c) / (dew_c + 243.5)),
    relative_humidity = (e / es) * 100, 
    e_pa = e *100, 
    rtioMoleDryH2O = e_pa / (pres_190m_pa - e_pa)
  )


write_csv(ERA5_pressure_corrected,
          "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/modelled_data/pressure_corrected.csv")






ERA5_pressure_corrected %>% 
  pivot_longer(cols = c(sp, pres_190m_pa), 
               names_to = "type", values_to = "pressure") %>% 
  ggplot(aes(valid_time, pressure, color = type))+
  geom_line() +
  theme_bw() +
  labs(x = "Date", y = "Pressure (Pa)")
  
ERA5_pressure_corrected %>% 
  ggplot(aes(valid_time, rtioMoleDryH2O))+
  geom_line() +
  theme_bw() 

bound_fluxes = read_csv("/mnt/scratch/users/cw1781/btt_flux_analysis/flux_analysis/data/flux_data/flux_all.csv")

fluxes_out_water = bound_fluxes %>% 
  select(hour, rtioMoleDryH2o)

modelled_water = ERA5_pressure_corrected %>% 
  rename("hour" = valid_time, 
         "rtioMoleDryH2O_model" = rtioMoleDryH2O)

join_both = left_join(modelled_water, fluxes_out_water, by = "hour")

join_both %>% 
  pivot_longer(cols = c(rtioMoleDryH2O_model, rtioMoleDryH2o), 
               names_to = "type", values_to = "water") %>% 
  ggplot(aes(hour, water, color = type)) +
  geom_line()

ggplotly()

