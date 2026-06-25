# library

library(tidyverse)
library(plotly)
library(paletteer)

# checking the modelled pressure data 


ERA5_pressure = read_csv("/mnt/longship/projects/chem-cmde-2019/btt_processing/raw_data/ERA5/reanalysis-era5-single-levels-timeseries-sfcf2tgx155.csv")


head(ERA5_pressure)

ggplot(ERA5_pressure, aes(valid_time, sp))+
  geom_line()


# Define Constants
g <- 9.80665
Rd <- 287.05
dz <- 190  # Height of BT Tower

ERA5_pressure_corrected = ERA5_pressure %>% 
  mutate(pres_190m_pa = sp * exp((-g * dz) / (Rd * t2m))) %>% 
  mutate(
    temp_c = t2m - 273.15,
    dew_c  = d2m - 273.15,
    # Saturation vapor pressure (es) and actual vapor pressure (e)
    es = 6.1094 * exp((17.625 * temp_c) / (temp_c + 243.04)),
    e  = 6.1094 * exp((17.625 * dew_c) / (dew_c + 243.04)),
    relative_humidity = (e / es) * 100, 
    e_pa = e *100, 
    rtioMoleDryH2O = e_pa / (pres_190m_pa - e_pa)
  )


write_csv(ERA5_pressure_corrected,
          "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/modelled_data/pressure_corrected.csv")



pressure_correction = ERA5_pressure_corrected %>% 
  # mutate(hour = hour(valid_time)) |> 
  # group_by(hour) |> 
  # summarise(sp = mean(sp), 
  #           pres_190m_pa = mean(pres_190m_pa)) |> 
  rename("Height corrected pressure" = pres_190m_pa, 
         "Surface pressure" = sp) |> 
  pivot_longer(cols = c("Surface pressure", "Height corrected pressure"), 
               names_to = "type", values_to = "pressure") %>% 
  ggplot(aes(valid_time, pressure, color = type))+
  geom_line() +
  theme_bw() +
  theme(
    strip.text             = element_text(size = 14),
    axis.title.x           = element_text(size = 14),
    axis.title.y           = element_text(size = 14),
    axis.text              = element_text(size = 12),
    axis.title.y.right     = element_text(size = 14, color = "black"),
    axis.text.y.right      = element_text(size = 12, color = "black")
  ) +
  scale_color_manual(values = c("#8CA252FF", #"#BD9E39FF", 
                                "grey")) +
  labs(x = "Date", y = "Pressure (Pa)", color = "")

ggsave(path = "/mnt/scratch/users/cw1781/btt_flux_analysis/flux_analysis/plots/thesis_plots", 
       plot = pressure_correction, device = "png", filename = "pressure_correction.png", 
       dpi = 300, width = 10, height = 6)
  

RH_rtio = ERA5_pressure_corrected %>% 
   mutate(day = floor_date(valid_time, "day")) |> 
   group_by(day) |> 
   summarise(relative_humidity = mean(relative_humidity), 
             rtioMoleDryH2O = mean(rtioMoleDryH2O)) |> 
  rename("Relative humidity (%)" = relative_humidity) |> 
  pivot_longer(cols = c("Relative humidity (%)", rtioMoleDryH2O), 
               values_to = "water", names_to = "type") |> 
  ggplot(aes(day, water, color = type))+
  geom_line() +
  theme_bw() +
  facet_wrap(~type, scales = "free_y", ncol = 1) +
  theme(
    strip.text             = element_text(size = 14),
    axis.title.x           = element_text(size = 14),
    axis.title.y           = element_text(size = 14),
    axis.text              = element_text(size = 12),
    axis.title.y.right     = element_text(size = 14, color = "black"),
    axis.text.y.right      = element_text(size = 12, color = "black"), 
    legend.position        = "none"
  ) +
  scale_color_manual(values = c("#D6616BFF", "#BD9E39FF")) +
  labs(x = "Date", y = "", color = "")

ggsave(path = "/mnt/scratch/users/cw1781/btt_flux_analysis/flux_analysis/plots/thesis_plots", 
       plot = RH_rtio, device = "png", filename = "RH_rtio.png", 
       dpi = 300, width = 8, height = 6)

ERA5_pressure_corrected %>% 
  mutate(hour = hour(valid_time)) |> 
  group_by(hour) |> 
  summarise(rtioMoleDryH2O = mean(rtioMoleDryH2O)) |> 
  ggplot(aes(hour, rtioMoleDryH2O))+
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

