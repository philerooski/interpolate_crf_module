library(tidyverse)
library(synapser)


fetch_gold_standard <- function() {
  gold_standard_f <- synGet("syn12257142")
  gold_standard <- gdata::read.xls(gold_standard_f$path) %>%
    as_tibble() %>% 
    mutate(Field.Date = lubridate::as_date(Field.Date),
           VO2.Date = lubridate::as_date(VO2.Date),
           PMI.ID = as.character(PMI.ID))
  return(gold_standard)
}

fetch_measurements <- function(external_ids) {
  measurements_q <- synTableQuery("select * from syn11665074")
  measurements <- measurements_q$asDataFrame() %>%
    select(-ROW_ID, -ROW_VERSION) %>% 
    filter(externalId %in% external_ids) %>% 
    mutate(createdOn = lubridate::as_datetime(createdOn / 1000),
           createdOn = createdOn + lubridate::hours(createdOnTimeZone / 100),
           uploadDate = lubridate::as_date(createdOn),
           externalId = as.character(externalId))
  return(measurements)
}

prep_measurements <- function(gold_standard, measurements) {
  gold_measure <- inner_join(
    gold_standard,
    measurements,
    by = c("PMI.ID" = "externalId", "Field.Date" = "uploadDate")) %>% 
    rename(actual_distance = X12.min.run.distance..m.) %>% 
    select(recordId, PMI.ID, phoneInfo, Field.Date, actual_distance)
  return(gold_measure)
}

fetch_location_data <- function() {
  q <- synTableQuery(paste(
    "select recordId, 'location.json' from syn11665074",
    "where externalId like 'PMI%'"))
  location_data <- q$asDataFrame() %>% select(recordId, location.json)
  paths <- synDownloadTableColumns(q, "location.json") %>% 
    bind_rows() %>% 
    gather(location.json, path) %>% 
    mutate(location.json = as.integer(location.json))
  location_data <- left_join(location_data, paths)
  return(location_data)
}

calculate_distances <- function(paths) {
  distances <- purrr::map_dfr(paths, function(p) {
    distance <- jsonlite::fromJSON(p)$coordinate %>%
      select(relativeLongitude, relativeLatitude) %>% 
      calculate_distance()
    return(distance)
  })
  distances_w_path <- distances %>% bind_cols(path = paths)
  return(distances_w_path)
}

calculate_distance <- function(coords) {
  meters_degree_lat <- 111321.5
  latitude_of_san_diego <- 32.7
  meters_degree_long <- cos(latitude_of_san_diego*pi/180) * meters_degree_lat
  coords_meters <- coords %>% 
    mutate(x = meters_degree_long * relativeLongitude,
           y = meters_degree_lat * relativeLatitude) %>% 
    select(x, y)
  tr <- trajr::TrajFromCoords(coords_meters)
  smoothed_tr <- trajr::TrajSmoothSG(tr, n = 19)
  tr_length <- tibble(estimated_distance = trajr::TrajLength(tr),
                      estimated_smoothed_distance = trajr::TrajLength(smoothed_tr))
  return(tr_length)
}

main <- function() {
  synLogin()
  gold_standard <- fetch_gold_standard()
  measurements <- fetch_measurements(gold_standard$PMI.ID)
  gold_measure <- prep_measurements(gold_standard, measurements)
  location_data <- fetch_location_data()
  gold_measure <- inner_join(gold_measure, location_data, by = "recordId")
}

#main()