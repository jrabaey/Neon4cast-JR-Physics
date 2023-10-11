generate_Jrabaey_forecast <- function(forecast_date, 
                                 forecast_name = 'GLEON_JRabaey_temp_physics',
                                 target_url,
                                 sites) {
  # 1.Read in the targets data
    targets <- readr::read_csv(target_url, guess_max = 10000) |> 
      mutate(observation = ifelse(observation == 0 & variable == "chla", 0.00001, observation)) %>%
      filter(variable == "temperature")
    
    
    # 2. Make the targets into a tibble with explicit gaps
    targets_ts <- targets %>%
      filter(datetime < forecast_date) |> 
      as_tsibble(key = c('variable', 'site_id'), index = 'datetime') %>%
      # add NA values up to today (index)
      tsibble::fill_gaps(.end = forecast_date)
    
    
    ## Helper fn: get daily average temperature from each ensemble in future
    noaa_mean_forecast <- function(site, var, reference_date) {
      endpoint = "data.ecoforecast.org"
      bucket <- glue::glue("neon4cast-drivers/noaa/gefs-v12/stage1/0/{reference_date}")
      s3 <- arrow::s3_bucket(bucket, endpoint_override = endpoint, anonymous = TRUE)
      
      # stage1 air temp is Celsius
      arrow::open_dataset(s3) |>
        dplyr::filter(site_id == site,
                      datetime >= lubridate::as_datetime(forecast_date),
                      variable == var) |>
        dplyr::select(datetime, prediction, parameter) |>
        dplyr::mutate(datetime = as_date(datetime)) |>
        dplyr::group_by(datetime, parameter) |>
        dplyr::summarize(air_temperature = mean(prediction), .groups = "drop") |>
        dplyr::select(datetime, air_temperature, parameter) |>
        dplyr::rename(ensemble = parameter) |>
        dplyr::collect()
      
    }
    

    forecast_site <- function(site) {
      message(paste0("Running site: ", site))
      
      site_target <- targets_ts |>
        dplyr::filter(site_id == site) |>
        tidyr::pivot_wider(names_from = "variable", values_from = "observation")
      
      if(is.na(site_target[nrow(site_target),]$temperature)){
        site_target[nrow(site_target),]$temperature <- site_target[nrow(site_target)-1,]$temperature
      }
      
      #  Get 30-day predicted temperature ensemble at the site
      noaa_future <- noaa_mean_forecast(site, "TMP", forecast_date)
      
      
      # Fit physics process model (water temp will equilibrate with air temp slowly)
      forecast_start <- as_tibble(tail(site_target, n =1))
      site_forecast <- NULL
      for(i in 1:30){
        forecast_data <- noaa_future[noaa_future$ensemble == i,]
        forecast_data <- bind_rows(select(forecast_start, -temperature, -site_id) %>% mutate(ensemble = i),
                               forecast_data)
        forecast_data$mod_temp_pred <- forecast_start$temperature
        
        for(i in 2:length(forecast_data$mod_temp_pred)){
          forecast_data$mod_temp_pred[i] <- forecast_data$mod_temp_pred[i-1] + 0.2 * 
            (forecast_data$air_temperature[i] - forecast_data$mod_temp_pred[i-1])
          if(!is.na(forecast_data$mod_temp_pred[i])){
            if(forecast_data$mod_temp_pred[i] <= 0){
              forecast_data$mod_temp_pred[i] <- 0.1
            }
          }
        }
        site_forecast <- dplyr::bind_rows(site_forecast, forecast_data[-1,])
      }
      temperature <- rename(site_forecast, prediction = "mod_temp_pred") %>%
        mutate(variable = "temperature")
      
      forecast <- dplyr::bind_rows(temperature)
      
      # Format results to EFI standard
      forecast <- forecast |>
        mutate(reference_datetime = forecast_date,
               site_id = site,
               family = "ensemble",
               model_id = forecast_name) |>
        rename(parameter = ensemble) |>
        select(model_id, datetime, reference_datetime,
               site_id, family, parameter, variable, prediction)
    }
    
    ### Run all sites -- may be slow!
    
    forecasts <- map_dfr(sites, forecast_site)
    
    # write forecast
    file_date <- forecasts$datetime[1]
    
    file_name <- paste0('aquatics-', file_date, '-', forecast_name, '.csv.gz')
    
    readr::write_csv(forecasts, file.path('Forecasts', file_name))  
    
    message(forecast_name, ' generated for ', forecast_date)
    
    
    valid <- neon4cast::forecast_output_validator(file.path('./Forecasts', file_name))
    
    
    if (!valid) {
      file.remove(file.path('./Forecasts/', file_name))
      message('forecast not valid')
    } else {
      return(file_name)
    }
    
  }