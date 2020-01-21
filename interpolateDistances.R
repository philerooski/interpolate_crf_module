library(tidyverse)
library(synapser)


fetch_gold_standard <- function() {
  gold_standard_f <- synGet("syn12257142")
  gold_standard <- gdata::read.xls(gold_standard_f$path) %>%
    as_tibble() %>% 
    mutate(Field.Date = lubridate::as_date(Field.Date),
           Lab.Date = lubridate::as_date(Lab.Date),
           PMI.ID = as.character(CRF.User.name))
  return(gold_standard)
}

fetch_measurements <- function(external_ids) {
  measurements_q <- synTableQuery("select * from syn11665074")
  measurements <- measurements_q$asDataFrame() %>%
    select(-ROW_ID, -ROW_VERSION) %>% 
    filter(externalId %in% external_ids) %>% 
    mutate(createdOnTimeZone = as.integer(createdOnTimeZone),
           createdOn = createdOn + lubridate::hours(createdOnTimeZone / 100),
           uploadDate = lubridate::as_date(createdOn),
           externalId = as.character(externalId))
  return(measurements)
}

prep_measurements <- function(gold_standard, measurements) {
  gold_measure <- dplyr::full_join(
    gold_standard,
    measurements,
    by = c("PMI.ID" = "externalId", "Field.Date" = "uploadDate")) %>% 
    rename(actual_distance = Total.Distance..m.) %>% 
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
    mutate(location.json = as.character(location.json))
  location_data <- left_join(location_data, paths)
  return(location_data)
}

calculate_distances <- function(recordId, phoneInfo, paths, smoothing_factor) {
  distances <- purrr::map2_dfr(phoneInfo, paths, function(pi, p) {
    if (is.na(p)) {
      tibble(estimated_distance = NA,
             estimated_smoothed_distance = NA)
    } else if (startsWith(pi, "iPhone")) {
      iphone_distance(p, smoothing_factor = smoothing_factor)
    } else {
      android_distance(p, smoothing_factor = smoothing_factor)  
    }
  })
  distances_w_rid <- distances %>% bind_cols(recordId = recordId)
  return(distances_w_rid)
}

android_distance <- function(p, smoothing_factor) {
  coords <- tryCatch({
    jsonlite::fromJSON(p)$coordinate %>%
      select(relativeLongitude, relativeLatitude)
  }, error = function(e) {
    tibble(estimated_distance = NA,
           estimated_smoothed_distance = NA)
  })
  if (!all(hasName(coords, c("relativeLongitude", "relativeLatitude")))) {
    return(coords) 
  }
  meters_degree_lat <- 111321.5
  latitude_of_san_diego <- 32.7
  meters_degree_long <- cos(latitude_of_san_diego*pi/180) * meters_degree_lat
  coords_meters <- coords %>% 
    mutate(x = meters_degree_long * relativeLongitude,
           y = meters_degree_lat * relativeLatitude) %>% 
    select(x, y)
  tr <- trajr::TrajFromCoords(coords_meters)
  smoothed_tr <- trajr::TrajSmoothSG(tr, n = smoothing_factor)
  tr_length <- tibble(estimated_distance = trajr::TrajLength(tr),
                      estimated_smoothed_distance = trajr::TrajLength(smoothed_tr))
  return(tr_length)
}

read_iphone <- function(path) {
  loc <- jsonlite::fromJSON(path) 
  if (!all(hasName(loc, c("timestamp", "course", "relativeDistance",
                          "verticalAccuracy", "horizontalAccuracy")))) {
    return(tibble(estimated_distance = NA, estimated_smoothed_distance = NA))
  }
  loc <- loc %>% 
    filter(stepPath == "Cardio 12MT/run/runDistance") %>% 
    mutate(timestamp = timestamp - min(timestamp)) %>%
    select(timestamp, course, relativeDistance,
           verticalAccuracy, horizontalAccuracy) %>% 
    mutate(phi = 90 - course,
           relative_x = cos(phi * pi/180) * relativeDistance,
           relative_y = sin(phi * pi / 180) * relativeDistance,
           x = cumsum(if_else(is.na(relative_x), 0, relative_x)),
           y = cumsum(if_else(is.na(relative_y), 0, relative_y))) %>% 
    select(timestamp, relative_x, relative_y, x, y, verticalAccuracy, horizontalAccuracy)
  return(loc)
}

iphone_distance <- function(p, smoothing_factor) {
  iphone_location_data <- read_iphone(p)
  if (!all(hasName(iphone_location_data, c("x", "y")))) {
    return(iphone_location_data)
  }
  tr <- trajr::TrajFromCoords(iphone_location_data[,c("x", "y")])
  smoothed_tr <- trajr::TrajSmoothSG(tr, n = smoothing_factor)
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
  estimated_distances <- calculate_distances(recordId = gold_measure$recordId,
                                             phoneInfo = gold_measure$phoneInfo,
                                             paths = gold_measure$path,
                                             smoothing_factor = 19)
  distance_comparison <- gold_measure %>% 
    left_join(estimated_distances, by = "recordId") %>% 
    select(-path, -location.json) %>% 
    rename(externalId = PMI.ID, field_date = Field.Date)
}

main()