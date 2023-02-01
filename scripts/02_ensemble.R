
var_input <- 
  
  c(# heat module
    "maximum_temperature",
    "minimum_temperature",
    "maximum_wetbulb_temperature",
    "average_temperature",
    
    # water module
    "precipitation",
    "precipitation+average_temperature",
    "precipitation+maximum_temperature",
    
    # land module
    "spei",
    "fwi")[8] # choose input variable to process




# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)

source("scripts/functions.R")

dir_derived <- "/mnt/bucket_mine/results/global_heat_pf/01_derived"
dir_ensembled <- "/mnt/bucket_mine/results/global_heat_pf/02_ensembled"

doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

wl <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")

# load thresholds table
thresholds <- 
  str_glue("/mnt/bucket_mine/misc_data/CMIP5_model_temp_thresholds.csv") %>% 
  read_delim() %>%
  suppressMessages() %>% 
  select(1:6) %>% 
  pivot_longer(-Model, names_to = "wl") %>% 
  mutate(wl = str_sub(wl, 3)) %>% 
  
  mutate(wl = ifelse(str_length(wl) == 1, str_glue("{wl}.0"), wl))  %>%
  
  mutate(Model = case_when(str_detect(Model, "HadGEM") ~ str_glue("MOHC-{Model}"),
                           str_detect(Model, "MPI") ~ str_glue("MPI-M-{Model}"),
                           str_detect(Model, "NorESM") ~ str_glue("NCC-{Model}"),
                           str_detect(Model, "GFDL") ~ str_glue("NOAA-GFDL-{Model}"),
                           str_detect(Model, "MIROC") ~ str_glue("MIROC-{Model}"),
                           TRUE ~ Model))

# load table of variables
tb_vars <-
  read_csv("/mnt/bucket_mine/pf_variable_table.csv") %>% 
  suppressMessages()

# derived vars to process
derived_vars <- 
  tb_vars %>% 
  filter(var_input == {{var_input}}) %>% 
  pull(var_derived)


# DOMAIN LOOP --------------------------------------------------------------------------------------


for(dom in doms){

  print(str_glue(" "))
  print(str_glue("PROCESSING {dom}"))
  
  
  # LOOP THROUGH VARS
  for(derived_var in derived_vars){
    
    print(str_glue(" "))
    print(str_glue("Processing {derived_var}"))
    
    
    # IMPORT
    
    #modifications to imported files
    change_import <- 
      tb_vars %>% 
      filter(var_derived == derived_var) %>% 
      pull(change_import)
    
    ff <- 
      dir_derived %>% 
      list.files() %>% 
      str_subset(dom) %>% 
      str_subset(derived_var)
    
    
    
    l_s <- 
      
      future_map(ff, function(f){
        
      
        if(change_import == "fix_date_convert_C"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
        } else if(change_import == "fix_date_convert_mm"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, kg/m^2/d))
          
        } else if(change_import == "convert_C"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
        } else if(change_import == "convert_mm"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, kg/m^2/d))
          
        } else if(change_import == "fix_date"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v")
          
        } else if(change_import == "nothing"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v")
          
        }
        
        
      },
      .options = furrr_options(seed = NULL)) %>% 
      
      # fix date dim
      map(function(s){
        
        # s %>% 
        #   st_set_dimensions("time",
        #                     values = st_get_dimension_values(s, "time") %>% 
        #                       as.character() %>% 
        #                       str_sub(end = 4)) %>% 
        #   drop_units()
        
        s %>%
          st_set_dimensions("time",
                            values = st_get_dimension_values(s, "time") %>%
                              # ymd() %>%
                              year()
                            ) %>%

        # s %>%
        #   st_set_dimensions("time",
        #                     values = seq(1970, length.out = dim(s)[3])
        #   ) %>%
        mutate(v = set_units(v, NULL))
      })
    
    
    # Verify correct import
    print(str_glue("Imported:"))
    
    walk2(l_s, ff, function(s, f){
      
      range_time <- 
        s %>% 
        st_get_dimension_values("time") %>% 
        range()
      
      f <- 
        f %>% 
        str_extract("(?<=yr_)[:alnum:]*_[:graph:]*(?=\\.nc)")
      
      print(str_glue("   Date range {f}:     \t{range_time[1]} - {range_time[2]}"))
      
    })
    
    
    
    
    # SLICE BY WL
    
    l_s_wl <- 
      
      map(wl, function(wl_){
        
        # wl_ <- "3.0"
        
        print(str_glue("Slicing WL {wl_}"))
        
        # l_s_wl <-
          
          map2(ff, l_s, function(f, s){
            
            # print(f)
            
            rcm_ <- f %>% str_split("_", simplify = T) %>% .[,4]
            gcm_ <- f %>% str_split("_", simplify = T) %>% .[,5] %>% str_remove(".nc")
            
            if(wl_ == "0.5"){
              
              s %>% 
                filter(time >= 1971,
                       time <= 2000)
              
              # print(str_glue("   1971-2000"))
              
            } else {
              
              thres_val <-
                thresholds %>%
                filter(str_detect(Model, str_glue("{gcm_}$"))) %>% 
                filter(wl == wl_)# %>%
                # pull(value)
              
              s <- 
                s %>% 
                filter(time >= thres_val$value - 10,
                       time <= thres_val$value + 10)
              
              print(str_glue("   {gcm_}: {thres_val$Model}: {thres_val$value}"))
              
              return(s)
            }
            
          }) %>% 
            {do.call(c, c(., along = "time"))}
        
      })
    
    
      
    l_s_wl_stats <-
      imap(wl, function(wl_, iwl){
        
        print(str_glue("Calculating stats WL {wl_}"))
        
        l_s_wl %>%
          pluck(iwl) %>%
          
          st_apply(c(1,2),
                   fn_statistics,
                   FUTURE = F,
                   .fname = "stats") %>%
          aperm(c(2,3,1)) %>%
          split("stats")
        
      })
      
    
    # WL as dimension
    s_result <-
      l_s_wl_stats %>%
      {do.call(c, c(., along = "wl"))} %>%
      st_set_dimensions(3, values = wl)
    
    
    
    # SAVE
    
    print(str_glue("Saving result"))
    
    res_filename <- 
      str_glue(
        "{dir_ensembled}/{dom}_{derived_var}_ensemble.nc"
      )
    
    fn_save_nc(res_filename, s_result)
    
    
  }
  
}



# map_dfr(doms, function(dom){
#   
#   ff <- 
#     dir_derived %>% 
#     list.files() %>% 
#     str_subset(dom) %>% #str_subset("wetbulb") %>% .[1] -> f
#     str_subset(derived_var)
#   
#   
#   map_dfr(ff, function(f){
#     
#     gcm_ <- f %>% str_split("_", simplify = T) %>% .[,5] %>% str_remove(".nc")
#     
#     
    # thresholds %>%
    #   filter(str_detect(Model, str_glue("{gcm_}$"))) %>%
    #   filter(wl == "1.0") %>%
    #   select(Model) %>%
    #   mutate(gcm = gcm_,
    #          dom = dom)
#     
#   })
#   
#   
# }) %>% View()
#   

