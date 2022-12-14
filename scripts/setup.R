
library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)


dir_bucket_cmip5 <- "/mnt/bucket_cmip5"
dir_bucket_mine <- "/mnt/bucket_mine"
dir_pers_disk <- "/mnt/pers_disk"


# str_glue("gcsfuse cmip5_data {dir_bucket_cmip5}") %>% 
#   system()
# 
# str_glue("gcsfuse clim_data_reg_useast1 {dir_bucket_mine}") %>% 
#   system()




# THRESHOLD TABLE ---------------------------------------------------------------------------------

str_glue("{dir_bucket_mine}/misc_data/CMIP5_model_temp_thresholds.csv") %>% 
  read_delim() %>% #filter(str_detect(Model, "MPI"))
  select(1:6) %>% 
  pivot_longer(-Model, names_to = "wl") %>% 
  mutate(wl = str_sub(wl, 3)) -> thresholds

thresholds %>% 
  mutate(wl = ifelse(str_length(wl) == 1, str_glue("{wl}.0"), wl)) -> thresholds

thresholds %>%
  mutate(Model = case_when(str_detect(Model, "HadGEM") ~ str_glue("MOHC-{Model}"),
                           str_detect(Model, "MPI") ~ str_glue("MPI-M-{Model}"),
                           str_detect(Model, "NorESM") ~ str_glue("NCC-{Model}"),
                           str_detect(Model, "GFDL") ~ str_glue("NOAA-GFDL-{Model}"),
                           str_detect(Model, "MIROC") ~ str_glue("MIROC-{Model}"),
                           TRUE ~ Model)) -> thresholds



# VARIABLE TABLE ----------------------------------------------------------------------------------

tb_vars <- 
  tibble(
    var_derived = c("days-gec-32C-tasmax",
                    "days-gec-35C-tasmax",
                    "days-gec-38C-tasmax",
                    "ten-hottest-days-tasmax",
                    "mean-tasmax",
                    "days-lec-0C-tasmax",
                    
                    "days-gec-20C-tasmin",
                    "days-gec-25C-tasmin",
                    "mean-tasmin",
                    "days-lec-0C-tasmin",
                    
                    "days-gec-26C-wetbulb",
                    "days-gec-28C-wetbulb",
                    "days-gec-30C-wetbulb",
                    "days-gec-32C-wetbulb",
                    "ten-hottest-days-wetbulb",
                    
                    "mean-tasmean",
                    
                    # *****************
                    
                    "total-precip",
                    "ninety-wettest-days",
                    "p98-precip"
    ),
    
    
    var_input = c("maximum_temperature",
                  "maximum_temperature",
                  "maximum_temperature",
                  "maximum_temperature",
                  "maximum_temperature",
                  "maximum_temperature",
                  
                  "minimum_temperature",
                  "minimum_temperature",
                  "minimum_temperature",
                  "minimum_temperature",
                  
                  "maximum_wetbulb_temperature",
                  "maximum_wetbulb_temperature",
                  "maximum_wetbulb_temperature",
                  "maximum_wetbulb_temperature",
                  "maximum_wetbulb_temperature",
                  
                  "average_temperature",
                  
                  # *******************
                  
                  "precipitation",
                  "precipitation",
                  "precipitation"
                  
    ),
    
    change_import = c("fix_date",
                      "fix_date",
                      "fix_date",
                      "fix_date_convert_C",
                      "fix_date_convert_C",
                      "fix_date",
                      
                      "fix_date",
                      "fix_date",
                      "fix_date_convert_C",
                      "fix_date",
                      
                      "fix_date",
                      "fix_date",
                      "fix_date",
                      "fix_date",
                      "fix_date", # already in C
                      
                      "fix_date_convert_C",
                      
                      # ***************
                      
                      "convert_mm",
                      "convert_mm",
                      "convert_mm"
                      
    ),
    
    final_name = c("days-above-32C",
                   "days-above-35C",
                   "days-above-38C",
                   "ten-hottest-days",
                   "average-daytime-temperature",
                   "freezing-days",
                   
                   "nights-above-20C",
                   "nights-above-25C",
                   "average-nighttime-temperature",
                   "frost-nights",
                   
                   "days-above-26C-wetbulb",
                   "days-above-28C-wetbulb",
                   "days-above-30C-wetbulb",
                   "days-above-32C-wetbulb",
                   "ten-hottest-wetbulb-days",
                   
                   "average-temperature",
                   
                   # ******************
                   
                   "change-total-annual-precipitation",
                   "change-90-wettest-days",
                   "change-hundred-yr-storm"
      
    )
  )

  