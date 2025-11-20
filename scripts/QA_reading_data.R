library(purrr)
library(plotly)

# QAing reading met data 

met_data <- list.files("/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/met_data_formatted/2024", 
                       pattern = "WXT", 
                       recursive = TRUE, 
                       full.names = TRUE)

met_data_04_25 <- list.files("/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/met_data_formatted/2025/04", 
                       pattern = "WXT", 
                       recursive = TRUE, 
                       full.names = TRUE)

met_data_09_25 <- list.files("/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/met_data_formatted/2025/09", 
                             pattern = "WXT", 
                             recursive = TRUE, 
                             full.names = TRUE)

#all_met_data <- map_df(met_data, read.csv)

all_met_data <- map_df(
  met_data,
  ~ read_csv(.x, col_types = cols(.default = col_character()))
) %>%
  type_convert()
  


all_met_data_04_25 <- map_df(
  met_data_04_25,
  ~ read_csv(.x, col_types = cols(.default = col_character()))
) %>%
  type_convert()

median(all_met_data_04_25$X11, na.rm = TRUE)

range(all_met_data_04_25$X11, na.rm = TRUE)


ggplot(all_met_data_04_25, aes(X11))+
  geom_density()


ggplotly()


all_met_data_09_25 <- map_df(
  met_data_09_25,
  ~ read_csv(.x, col_types = cols(.default = col_character()))
) %>%
  type_convert()

median(all_met_data_09_25$X11)


ggplot(all_met_data_07_25, aes(X11))+
  geom_density()

ggplotly()

# Looking at other variables - where is RH? 

ggplot(all_met_data_11_24, aes(X10))+
  geom_density()

example_edit <- all_met_data_07_25 %>% 
  # mutate(
  #   datetime = as.POSIXct(date, format = "%Y-%m-%d %H:%M:OS", tz = "UTC"), 
  #   relative_humidity = backcalc_RH(T_air, p_air, FD_mole_H2O), 
  #   presAtm = p_air
  # ) %>% 
  mutate(
  presAtm = ifelse(X11 < 900 | X11 > 1100, NA, X11)  # remove bad values
) %>% 
  mutate(presAtm = zoo::na.locf(presAtm, na.rm = FALSE))


ggplot(example_edit, aes(presAtm))+
  geom_density()


# LOCF fill for pressure only
df_reading$presAtm <- zoo::na.locf(df_reading$presAtm, na.rm = FALSE)


# QAing Sam met data 

sam_example <- read_csv("/mnt/scratch/projects/chem-cmde-2019/btt_processing/sam_input_data/2023/150/NOx_5Hz_23_05_30_030000.csv") %>% 
  mutate(
    datetime = as.POSIXct(date, format = "%Y-%m-%d %H:%M:OS", tz = "UTC"), 
    relative_humidity = backcalc_RH(T_air, p_air, FD_mole_H2O), 
    presAtm = p_air
  )

ggplot(sam_example, aes(presAtm))+
  geom_density()


