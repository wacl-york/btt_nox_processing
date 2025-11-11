
library(arrow)
library(dplyr)
library(purrr)
library(ggplot2)
library(patchwork)
library(shiny)

out_dir <- "/mnt/scratch/projects/chem-cmde-2019/btt_processing/processing/1Hz_cal_data"

# Find all parquet files recursively
processed_files <- list.files(out_dir, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)

qc_summary <- map_dfr(processed_files, function(f) {
  df <- tryCatch(read_parquet(f), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0) return(NULL)
  
  tibble(
    file = basename(f),
    n_rows = nrow(df),
    ce_min = min(df$ce, na.rm = TRUE),
    ce_max = max(df$ce, na.rm = TRUE),
    ch1_sens_mean = mean(df$ch1_sens, na.rm = TRUE),
    ch2_sens_mean = mean(df$ch2_sens, na.rm = TRUE),
    ch1_sens_na = sum(is.na(df$ch1_sens)),
    ch2_sens_na = sum(is.na(df$ch2_sens))
  )
})

qc_summary





qc_plot_dir <- file.path(out_dir, "qc_plots")
dir.create(qc_plot_dir, showWarnings = FALSE)

for (f in processed_files) {
  message("Plotting QC for: ", basename(f))
  df <- tryCatch(read_parquet(f), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0) next
  
  p_sens <- ggplot(df, aes(x = datetime)) +
    geom_line(aes(y = ch1_sens, color = "Ch1"), linewidth = 0.7) +
    geom_line(aes(y = ch2_sens, color = "Ch2"), linewidth = 0.7) +
    labs(title = paste("Sensitivities:", basename(f)), y = "Sensitivity", x = "Datetime") +
    scale_color_manual(values = c("Ch1" = "darkgreen", "Ch2" = "purple")) +
    theme_minimal()
  
  p_press <- ggplot(df, aes(x = av_rxn_vessel_pressure)) +
    geom_point(aes(y = ch1_sens, color = "Ch1"), alpha = 0.3) +
    geom_point(aes(y = ch2_sens, color = "Ch2"), alpha = 0.3) +
    labs(x = "Pressure", y = "Sensitivity") +
    theme_minimal()
  
  ggsave(
    filename = file.path(qc_plot_dir, paste0(tools::file_path_sans_ext(basename(f)), "_qc.png")),
    plot = (p_sens / p_press),
    width = 9, height = 6, dpi = 150
  )
}




processed_files <- list.files(out_dir, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)

ui <- fluidPage(
  titlePanel("Calibration QC Dashboard"),
  selectInput("file", "Select a File:", choices = processed_files),
  plotOutput("sens_plot", height = "350px"),
  plotOutput("press_plot", height = "350px")
)

server <- function(input, output, session) {
  df_data <- reactive({
    read_parquet(input$file)
  })
  
  output$sens_plot <- renderPlot({
    df <- df_data()
    ggplot(df, aes(x = datetime)) +
      geom_line(aes(y = ch1_sens, color = "Ch1")) +
      geom_line(aes(y = ch2_sens, color = "Ch2")) +
      labs(title = "Sensitivities Over Time", x = "Datetime", y = "Sensitivity") +
      scale_color_manual(values = c("Ch1" = "darkgreen", "Ch2" = "purple")) +
      theme_minimal()
  })
  
  output$press_plot <- renderPlot({
    df <- df_data()
    ggplot(df, aes(x = av_rxn_vessel_pressure)) +
      geom_point(aes(y = ch1_sens, color = "Ch1"), alpha = 0.4) +
      geom_point(aes(y = ch2_sens, color = "Ch2"), alpha = 0.4) +
      labs(title = "Sensitivity vs Pressure", x = "Pressure", y = "Sensitivity") +
      theme_minimal()
  })
}

shinyApp(ui, server)


#  instead looking at hourly interpolations 

process_hourly <- function(f) {
  
  message("Processing: ", basename(f))
  
  # Read parquet and lowercase column names
  df <- open_dataset(f, format = "parquet") %>%
    collect() %>%
    rename_all(tolower) %>%
    rename("av_rxn_vessel_pressure" = rxn_vessel_pressure) %>%
    mutate(datetime = parse_excel_date(thetime)) %>%
    arrange(datetime)
  
  if (nrow(df) == 0) return(NULL)
  
  # -------------------------------
  # 1. Downsample to hourly resolution
  # -------------------------------
  df_hourly <- df %>%
    mutate(datetime = floor_date(datetime, "hour")) %>%
    group_by(datetime) %>%
    summarize(
      av_rxn_vessel_pressure = mean(av_rxn_vessel_pressure, na.rm = TRUE),
      inlet_pressure = mean(inlet_pressure, na.rm = TRUE),
      .groups = "drop"
    )
  
  # -------------------------------
  # 2. Interpolate CE from global calibration
  # -------------------------------
  df_hourly <- df_hourly %>%
    mutate(
      ce = approx(ce_interp$date, ce_interp$ce, xout = datetime, rule = 2)$y
    )
  
  # -------------------------------
  # 3. Assign periods
  # -------------------------------
  df_hourly <- df_hourly %>%
    mutate(period = get_period(datetime)) %>%
    left_join(coef_tbl_both_pressure, by = "period") %>%
    arrange(datetime)
  
  # -------------------------------
  # 4. Compute raw sensitivities based on pressure
  # -------------------------------
  df_hourly <- df_hourly %>%
    mutate(
      ch1_sens_raw = case_when(
        period %in% lm_periods &
          av_rxn_vessel_pressure <= 40 &
          av_rxn_vessel_pressure >= p_min &
          av_rxn_vessel_pressure <= p_max ~
          intercept_ch1 + slope_ch1 * av_rxn_vessel_pressure,
        TRUE ~ NA_real_
      ),
      ch2_sens_raw = case_when(
        period %in% lm_periods &
          av_rxn_vessel_pressure <= 40 &
          av_rxn_vessel_pressure >= p_min &
          av_rxn_vessel_pressure <= p_max ~
          intercept_ch2 + slope_ch2 * av_rxn_vessel_pressure,
        TRUE ~ NA_real_
      ),
      
      # Interpolate missing sensitivities across time
      ch1_sens = zoo::na.approx(ch1_sens_raw, x = datetime, na.rm = FALSE, rule = 2),
      ch2_sens = zoo::na.approx(ch2_sens_raw, x = datetime, na.rm = FALSE, rule = 2)
    )
  
  # -------------------------------
  # 5. Apply physical filters
  # -------------------------------
  df_hourly <- df_hourly %>%
    mutate(
      ch1_sens = ifelse(
        av_rxn_vessel_pressure > 40 |
          inlet_pressure < 199 |
          inlet_pressure > 300, NA_real_, ch1_sens
      ),
      ch2_sens = ifelse(
        av_rxn_vessel_pressure > 40 |
          inlet_pressure < 199 |
          inlet_pressure > 300, NA_real_, ch2_sens
      ),
      ce = ifelse(
        av_rxn_vessel_pressure > 40 |
          inlet_pressure < 199 |
          inlet_pressure > 300, NA_real_, ce
      )
    ) %>%
    select(datetime, av_rxn_vessel_pressure, inlet_pressure, ce, ch1_sens, ch2_sens)
  
  return(df_hourly)
}

# -------------------------------
# 6. Process all parquet files
# -------------------------------
param_files <- list.files(data_root, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)

hourly_list <- lapply(param_files, process_hourly)

# Combine into one table
hourly_df <- bind_rows(hourly_list) %>% arrange(datetime)


ggplot(hourly_df, aes(x = datetime)) +
  geom_line(aes(y = ch1_sens, color = "Ch1 Sens")) +
  geom_line(aes(y = ch2_sens, color = "Ch2 Sens")) +
  geom_line(aes(y = ce * 10, color = "CE x10"), linetype = "dashed") +
  labs(y = "Sensitivity / CE (scaled)", color = "Variable") +
  theme_minimal()


