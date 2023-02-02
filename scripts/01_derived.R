
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
    "fwi")[1] # choose input variable to process




# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)

source("scripts/functions.R")

dir_cordex <- "/mnt/bucket_cmip5/RCM_regridded_data"
dir_derived <- "/mnt/bucket_mine/results/global_heat_pf/01_derived"
dir_tmp <- "/mnt/pers_disk"

doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

# load table of variables
tb_vars <-
  read_csv("/mnt/bucket_mine/pf_variable_table.csv") %>% 
  suppressMessages()




# DOMAIN LOOP -----------------------------------------------------------------

for(dom in doms){
  
  print(str_glue(" "))
  print(str_glue("PROCESSING {dom}"))
  
  # assemble table of needed files for calculation
  tb_files <-
    str_split(var_input, "[+]") %>% .[[1]] %>% # in case > 1 variable 
    map_dfr(fn_data_table)
  
  # extract models
  tb_models <-
    unique(tb_files[, c("gcm", "rcm")])
  
  # ignore reggie in these domains  
  if(dom %in% c("SAM", "AUS", "CAS")){
    
    tb_models <- 
      tb_models %>% 
      filter(str_detect(rcm, "RegCM", negate = T))
    
  }
  
  
  
  
  # MODEL LOOP ----------------------------------------------------------------
  
  for(i in seq_len(nrow(tb_models))){
    
    print(str_glue(" "))
    print(str_glue("PROCESSING model {i} / {nrow(tb_models)}"))
    
    gcm_ <- tb_models$gcm[i]
    rcm_ <- tb_models$rcm[i]
    
    
    ## DOWNLOAD RAW DATA ------------------------------------------------------
    
    print(str_glue("Downloading raw data [{rcm_}] [{gcm_}]"))
    
    dir_raw_data <- str_glue("{dir_tmp}/raw_data")
    
    if(dir.exists(dir_raw_data)){
      print(str_glue("   (previous dir_raw_data deleted)"))
      unlink(dir_raw_data, recursive = T)
    } 
    
    dir.create(dir_raw_data)
    
    # table of files for 1 model
    tb_files_mod <- 
      tb_files %>% 
      filter(gcm == gcm_,
             rcm == rcm_) 
    
    # download files in parallel  
    tb_files_mod %>% 
      future_pwalk(function(loc, file, ...){
        
        loc_ <-
          loc %>% 
          str_replace("/mnt/bucket_cmip5", "gs://cmip5_data")
        
        str_glue("{loc_}/{file}") %>%
          {system(str_glue("gsutil cp {.} {dir_raw_data}"), 
                  ignore.stdout = T, ignore.stderr = T)}
        
      })
    
    "   Done: {length(list.files(dir_raw_data))} / {nrow(tb_files_mod)} files downloaded" %>% 
      str_glue() %>% 
      print()
    
    
    
    ## CDO PRE-PROCESS --------------------------------------------------------
    
    # CDO is used here to split files annually and fix their time dimension.
    # This process solves some inconsistencies present in RegCM4 models.
    # Then files are concatenated again into one single file with the complete
    # record of years (~ 1970-2100). Concatenating everything into one file 
    # eases successive calculations to obtain derived variables.
    
    
    print(str_glue("Pre-processing with CDO [{rcm_}] [{gcm_}]"))
    
    tb_files_mod %>%
      future_pwalk(function(file, t_i, t_f, ...){
        
        # extract first and last year included in the file
        yr_i <- year(as_date(t_i))
        yr_f <- year(as_date(t_f))
        
        f <- str_glue("{dir_raw_data}/{file}")
        
        
        if(str_detect(var_input, "wetbulb")){ # already split annually
          
          f_new <- str_glue("{dir_raw_data}/yrfix_{yr_i}.nc")
          
          # fix time
          system(str_glue("cdo -a setdate,{yr_i}-01-01 {f} {f_new}"),
                 ignore.stdout = T, ignore.stderr = T)
          
          file.remove(f)
          
          
        } else {
          
          # extract variable's (short) name
          v <- str_split(file, "_") %>% .[[1]] %>% .[1]
          
          # split annually
          system(str_glue("cdo splityear {f} {dir_raw_data}/{v}_yrsplit_"),
                 ignore.stdout = T, ignore.stderr = T)
          
          # fix time (only of the files that came from the file above)
          dir_raw_data %>% 
            list.files(full.names = T) %>% 
            str_subset(v) %>% 
            str_subset("yrsplit") %>%
            str_subset(str_flatten(yr_i:yr_f, "|")) %>% 
            
            walk2(seq(yr_i, yr_f), function(f2, yr){
              
              f_new <- str_glue("{dir_raw_data}/{v}_yrfix_{yr}.nc")
              
              system(str_glue("cdo -a setdate,{yr}-01-01 {f2} {f_new}"),
                     ignore.stdout = T, ignore.stderr = T)
              
              file.remove(f2)
              
            })
          
          file.remove(f)
          
        }
        
        
      })
    
    
    # check if some files could not be year-split/time-fixed
    bad_remnants <- 
      dir_raw_data %>% 
      list.files(full.names = T) %>% 
      str_subset("yrsplit")
    
    if(length(bad_remnants) > 0){
      print(str_glue("   ({length(bad_remnants)} bad files - deleted)"))
      
      bad_remnants %>% 
        walk(file.remove)
    }
    
    
    # concatenate
    
    dir_cat <- str_glue("{dir_tmp}/cat")
    
    if(dir.exists(dir_cat)){
      print(str_glue("   (previous dir_cat deleted)"))
      unlink(dir_cat, recursive = T)
    }
    
    dir.create(dir_cat)
    
    # extract variable(s)'s (short) name
    v <- 
      dir_raw_data %>%
      list.files() %>% 
      str_split("_", simplify = T) %>% 
      .[,1] %>% 
      unique()
    
    # loop through variable(s)
    future_walk(v, function(vv){
      
      ff <-
        dir_raw_data %>%
        list.files(full.names = T) %>%
        str_subset(str_glue("{vv}_")) %>% 
        str_flatten(" ")
      
      system(str_glue("cdo -settunits,d  -cat {ff} {dir_cat}/{vv}_cat.nc"),
             ignore.stdout = T, ignore.stderr = T)
      
    })
    
    
    ## CALCULATE DERIVED VARIABLES --------------------------------------------
    
    # This section calculates derived variables from the concatenated file.
    # All derived variables that use the same input var are calculated at once. 
    # This section calls another script (derived_fns.R) that contains all the 
    # functions to perform the necessary calculations depending on the derived
    # variable to obtain.
    
    
    print(str_glue("Processing derived variables [{rcm_}] [{gcm_}]"))
    
    # identify vars to derive from input var
    derived_vars <- 
      tb_vars %>%
      filter(var_input == {{var_input}}) %>% 
      pull(var_derived)  
    
    # loop through derived vars
    for(derived_var in derived_vars){
    
      print(str_glue("   {derived_var}"))
      
      outfile <-
        str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}.nc")
      
      if(file.exists(outfile)){
        file.remove(outfile)
        print(str_glue("      (replacing derived file)"))
      }
      
      # call script with functions
      source("scripts/derived_fns.R")
      
      # verify correct time dimension
      time_steps <- 
        str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}.nc") %>% 
        read_ncdf(proxy = T, make_time = F) %>%
        suppressMessages() %>% 
        suppressWarnings() %>% 
        st_get_dimension_values("time") %>% 
        length()
      
      print(str_glue("      Done: new file with {time_steps} timesteps"))
      
    }
    
    # delete concatenated file
    unlink(dir_cat, recursive = T)
    
    # delete raw files
    unlink(dir_raw_data, recursive = T)
    
  }
  
}

