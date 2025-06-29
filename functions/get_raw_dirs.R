get_raw_dirs = function(){
  
  config = ini::read.ini("/btt_nox_processing/config.ini")
  
  dirs = list.dirs(config$paths$raw_data) 
  dirs = dplyr::tibble(dir = stringr::str_remove(dirs, config$paths$raw_data))
  dirs = dirs |> 
    dplyr::filter(stringr::str_detect(dir, "/") & !stringr::str_detect(dir, "Conflict")) |> 
    tidyr::separate_wider_delim(cols = dir, delim = "/", names = c("yyy","type", "year", "month"), too_few = "align_start") |> 
    dplyr::select(-yyy) |> 
    dplyr::filter(
      !is.na(month),
      type %in% c("five_hz", "params")
    )
  
  dirs
}