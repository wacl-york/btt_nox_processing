library(tidyverse)
library(arrow)
library(waclr)
library(plotly)


file <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/raw_parquet/data/params_2/2025/param_2025_04.parquet"

df <- open_dataset(file, format = "parquet") %>%
  collect() %>%
  mutate(datetime = parse_excel_date(TheTime)) %>%
  rename_all(tolower) %>% 
  rename("av_rxn_vessel_pressure" = rxn_vessel_pressure) %>% 
  arrange(datetime)

zero_data <- df %>%
  filter(zero_valve_1 == 1.0, zero_valve_2 == 1.0) %>%
  mutate(second = second(datetime)) %>% 
  filter(between(second, 5, 15)) 
  select(date = datetime, ch1_hz, ch2_hz) %>%
  arrange(date)

zero_data %>% 
  pivot_longer(cols = c(ch1_hz, ch2_hz), 
               names_to = "channel", values_to = "counts") %>% 
  ggplot(aes(datetime, counts, color = channel))+
  geom_line()

ggplotly()

check <- df %>% 
  mutate(date_hour = floor_date(datetime, "hour")) %>% 
  filter(date_hour == "2021-06-07 12:00:00") %>% 
  mutate(second = second(datetime)) %>%
  filter(zero_valve_1 == 1.0, zero_valve_2 == 1.0) %>%
  filter(between(second, 5, 15))

check %>% 
  mutate(second = second(datetime)) %>%
  filter(zero_valve_1 == 1.0, zero_valve_2 == 1.0) %>%
  filter(between(second, 5, 15)) %>% 
  pivot_longer(cols = c(ch1_hz, ch2_hz), 
               names_to = "channel", values_to = "counts") %>% 
ggplot(aes(datetime, counts, color = channel))+
  geom_line()

ggplotly()



zero_data <- df %>%
  filter(zero_valve_1 == 1.0, zero_valve_2 == 1.0) %>%
  mutate(second = second(datetime), 
         hour_block = floor_date(datetime, "hour")) %>%
  filter(between(second, 5, 15)) %>%
  filter(ch2_hz > 1200) %>% 
  filter(ch1_hz > 1200) %>% 
  group_by(hour_block) %>% # we only want the zeros from the start of the hour (when we do a cal there are multiple zeros)
  summarise(
    date = min(datetime),
    ch1_hz = median(ch1_hz, na.rm = TRUE), #take the median of the zero and interpolate that 
    ch2_hz = median(ch2_hz, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(date)

zero_data %>% 
  pivot_longer(cols = c(ch1_hz, ch2_hz), 
               names_to = "channel", values_to = "counts") %>%
  ggplot(aes(date, counts, color = channel))+
  geom_line()

df_edit <- df %>%
  mutate(ch1_zero = approx(zero_data$date, zero_data$ch1_hz, xout = datetime, rule = 2)$y,
    ch2_zero = approx(zero_data$date, zero_data$ch2_hz, xout = datetime, rule = 2)$y)

ggplot(df_edit, aes(datetime, ch1_zero))+
  geom_line()

ggplotly()

