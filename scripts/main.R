
source("scripts/setup.R")
source("scripts/functions.R")

plan(multicore)


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



# DOMAIN LOOP -------------------------------------------------------------------------------------


for(dom in c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR")){
  
  print(str_glue("PROCESSING {dom}"))
  
  # DATA TABLE --------------------------------------------------------------------------------------

  dir_remo <- str_glue("{dir_bucket_cmip5}/RCM_regridded_data/REMO2015/{dom}/daily/maximum_temperature")
  dir_regcm <- str_glue("{dir_bucket_cmip5}/RCM_regridded_data/CORDEX_22/{dom}/daily/maximum_temperature")
  
  tb_files <- 
    map_dfr(c(dir_remo, dir_regcm), function(dd){
      
      dd %>% 
        list.files() %>%
        .[str_length(.) > 80] %>% 
        
        map_dfr(function(d){
          
          tibble(file = d) %>%
            
            mutate(
              
              var = file %>%
                str_split("_", simplify = T) %>%
                .[ , 1],
              
              gcm = file %>%
                str_split("_", simplify = T) %>%
                .[ , 3],
              
              rcm = file %>%
                str_split("_", simplify = T) %>%
                .[ , 6] %>% 
                str_split("-", simplify = T) %>% 
                .[ , 2],
              
              t_i = file %>%
                str_split("_", simplify = T) %>%
                .[ , 9] %>%
                str_sub(end = 6) %>% 
                str_c("01"),
              
              t_f = file %>%
                str_split("_", simplify = T) %>%
                .[ , 9] %>%
                str_sub(start = 10, end = 16) %>% 
                str_c("01"),
              
              loc = dd
              
            )
        }) 
    }) %>% 
    
    filter(year(as_date(t_i)) >= 1970) %>% 
    filter(
      str_detect(gcm, "EC-EARTH", negate = T),
      str_detect(gcm, "CERFACS", negate = T),
      str_detect(gcm, "CNRM", negate = T)
    )
  
  
  
  
  
  # MODELS TABLE ------------------------------------------------------------------------------------
  
  tb_models <- 
    unique(tb_files[, c("gcm", "rcm")]) %>% 
    mutate(calendar = case_when(str_detect(gcm, "Had") ~ 360,
                                str_detect(rcm, "REMO") & str_detect(gcm, "Had", negate = T) ~ 365.25,
                                str_detect(rcm, "RegCM") & str_detect(gcm, "MPI") ~ 365.25,
                                str_detect(rcm, "RegCM") & str_detect(gcm, "Nor") ~ 365,
                                str_detect(rcm, "RegCM") & str_detect(gcm, "GFDL") ~ 365))
  
  
  if(dom %in% c("SAM", "AUS", "CAS")){
    
    tb_models <- 
      tb_models %>% 
      filter(str_detect(rcm, "RegCM", negate = T))
    
  }
  
  
  # LOOP THROUGH MODELS -----------------------------------------------------------------------------
  
  dir_derived <- str_glue("{dir_bucket_mine}/results/global_heat_pf/01_derived")
  # dir.create(dir_derived)
  
  
  for(i in seq_len(nrow(tb_models))){
    
    gcm_ <- tb_models$gcm[i]
    rcm_ <- tb_models$rcm[i]
    
    
    
    ## DOWNLOAD RAW DATA ----------------------------------------------------------------------------
    
    print(str_glue("Downloading raw data [{rcm_}] [{gcm_}]"))
    
    dir_raw_data <- str_glue("{dir_pers_disk}/raw_data")
    dir.create(dir_raw_data)
    
    tb_files %>% 
      filter(gcm == gcm_,
             rcm == rcm_) %>% 
      
      future_pwalk(function(loc, file, ...){
        
        loc_ <-
          loc %>% 
          str_replace("/mnt/bucket_cmip5", "gs://cmip5_data")
        
        str_glue("{loc_}/{file}") %>%
          {system(str_glue("gsutil cp {.} {dir_raw_data}"), 
                  ignore.stdout = T, ignore.stderr = T)}
        
      })
    
    
    
    
    ## CDO PRE-PROCESS -------------------------------------------------------------------------------
    
    print(str_glue("Processing with CDO [{rcm_}] [{gcm_}]"))
    
    ff <-
      dir_raw_data %>%
      list.files(full.names = T) %>%
      str_flatten(" ")
    
    
    # 3 count days > thres
    
    print(str_glue("   Count days above thresh"))
    
    {
      lim_c <- c(32, 35, 38)
      
      future_walk(lim_c, function(lim_c_){
        
        set_units(lim_c_, degC) %>% 
          set_units(K) %>% 
          drop_units() -> lim_k
        
        outfile <-
          str_glue("{dir_derived}/{dom}_days-gec-{lim_c_}C_yr_{rcm_}_{gcm_}.nc")
        
        str_glue("cdo -yearsum -gec,{lim_k} -cat {ff} {outfile}") %>%
          system(ignore.stdout = T, ignore.stderr = T)
        
      })
    }
    
    
    # 10 hottest days
    
    print(str_glue("   10 hottest days"))
    
    {
      
      dir_temp <- "/mnt/pers_disk/dir_temp"
      dir.create(dir_temp)
      
      # concatenate all; then split per year
      str_glue("cdo -splityear -cat {ff} {dir_temp}/tempyr_") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
      
      # sort time; extract top 10; calculate mean
      dir_temp %>% 
        list.files() %>% 
        str_subset("^tempyr_") %>% 
        future_walk(function(fff){
          
          str_glue("cdo -timsort {dir_temp}/{fff} {dir_temp}/sort_{fff}") %>% 
            system(ignore.stdout = T, ignore.stderr = T)
          
          file.remove(str_glue("{dir_temp}/{fff}"))
          
          time_length <- 
            str_glue("{dir_temp}/sort_{fff}") %>% 
            read_ncdf(proxy = T, make_time = F) %>% 
            suppressMessages() %>% 
            dim() %>% 
            .[3]
          
          str_glue("cdo -timmean -seltimestep,{time_length-9}/{time_length} {dir_temp}/sort_{fff} {dir_temp}/mean_sel_{fff}") %>%
            system(ignore.stdout = T, ignore.stderr = T)
          
          file.remove(str_glue("{dir_temp}/sort_{fff}"))
          
        })
      
      # concatenate
      dir_temp %>% 
        list.files(full.names = T) %>% 
        str_subset("/mean_") %>% 
        str_flatten(" ") %>% 
        
        {system(str_glue("cdo cat {.} {dir_derived}/{dom}_ten-hottest-days_yr_{rcm_}_{gcm_}.nc"),
                ignore.stdout = T, ignore.stderr = T)}
      
      # delete intermediate files
      unlink(dir_temp, recursive = T)
      
    }
    
    
    # Daytime temp
    
    print(str_glue("   Daytime temp"))
    
    {
      outfile <-
        str_glue("{dir_derived}/{dom}_mean-tasmax_yr_{rcm_}_{gcm_}.nc")
      
      str_glue("cdo -yearmean -cat {ff} {outfile}") %>%
        system(ignore.stdout = T, ignore.stderr = T)
    }
    
    
    ## DELETE RAW FILES -----------------------------------------------------------------------------
    
    unlink(dir_raw_data, recursive = T)
    
  }
  
  
  
  
  # ENSEMBLE ----------------------------------------------------------------------------------------
  
  # tb_derived_vars <- 
  #   tibble(
  #     var_name = c("days-gec-32C",
  #                  "days-gec-35C",
  #                  "days-gec-38C",
  #                  "ten-hottest-days",
  #                  "mean-tasmax")
  #   )
  
  derived_vars <- c("days-gec-32C",
                    "days-gec-35C",
                    "days-gec-38C",
                    "ten-hottest-days",
                    "mean-tasmax")
  
  wl <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")
  
  
  
  # LOOP THROUGH VARS
  walk(derived_vars, function(derived_vars_){
    
    # derived_vars_ <- derived_vars[1]
    
    print(str_glue(" "))
    print(str_glue("Processing {derived_vars_}"))
    
    
    l_s <- 
      
      # import
      pmap(tb_models, function(rcm, gcm, ...){
        
        f <- 
          dir_derived %>% 
          list.files(full.names = T) %>%
          str_subset(dom) %>% 
          str_subset(derived_vars_) %>% 
          str_subset(gcm) %>% 
          str_subset(rcm)
        
        # ten-hottest-days lost its time values
        if(derived_vars_ == "ten-hottest-days"){
          read_ncdf(f, proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
        } else if(derived_vars_ == "mean-tasmax") {
          read_ncdf(f, proxy = F) %>% 
            suppressMessages() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
        } else {
          read_ncdf(f, proxy = F) %>% 
            suppressMessages() %>% 
            setNames("v")
          
        }
        
        
      }) %>% 
      
      # fix date dim
      map(function(s){
        
        s %>% 
          st_set_dimensions("time",
                            values = st_get_dimension_values(s, "time") %>% 
                              as.character() %>% 
                              str_sub(end = 4)) %>% 
          drop_units()
      })
    
    
    
    
    # LOOP THROUGH WL
    
    l_s_wl <- 
      map(wl, function(wl_){
        
        # wl_ <- "3.0"
        
        print(str_glue("Processing WL {wl_}"))
        
        
        # SLICE WL
        
        l_s_wl <- 
          future_map2(l_s, seq_along(l_s), function(s, i){
            
            rcm_ <- tb_models$rcm[i]
            gcm_ <- tb_models$gcm[i]
            
            if(wl_ == "0.5"){
              
              s %>% 
                filter(time >= 1971,
                       time <= 2000)
              
            } else {
              
              thres_val <- 
                thresholds %>% 
                filter(Model == gcm_) %>% 
                filter(wl == wl_) %>% 
                pull(value)
              
              s %>% 
                filter(time >= thres_val - 10,
                       time <= thres_val + 10)
              
            }
            
          })
        
        
        # CALCULATE STATS
        
        l_s_wl %>% 
          {do.call(c, c(., along = "time"))} %>%
          
          st_apply(c(1,2), 
                   fn_statistics,
                   FUTURE = T,
                   .fname = "stats") %>% 
          aperm(c(2,3,1)) %>% 
          # mutate(v = round(v)) %>% 
          split("stats")
        
      })
    
    
    s_result <- 
      l_s_wl %>% 
      {do.call(c, c(., along = "wl"))} %>% 
      st_set_dimensions(3, values = wl)
    
    
    # SAVE
    fn_write_nc_wtime_wvars(
      s_result,
      str_glue(
        "{dir_bucket_mine}/results/global_heat_pf/02_ensembled/{dom}_{derived_vars_}_ensemble.nc"
      )
    )
    
  })
  
  
  
  
}































# TILING ------------------------------------------------------------------------------------------

f <-
  dir_remo %>%
  list.files(full.names = T) %>%
  str_subset("_2000.nc") %>% 
  str_subset("Had") %>% 
  .[1]

source("scripts/tiling.R")



# LOOP THROUGH TILES ------------------------------------------------------------------------------

calendar_type <- 
  tb_models %>% 
  filter(gcm == gcm_,
         rcm == rcm_ %>% str_split("-", simplify = T) %>% .[,2]) %>% 
  pull(calendar)



for(i in seq_len(nrow(chunks_ind))){
  
  i <- 24
  
  
  print(str_glue(" "))
  print(str_glue("*******************"))
  print(str_glue("PROCESSING TILE {i}"))
  
  r <- chunks_ind$r[i]
  lon_ch <- chunks_ind$lon_ch[i]
  lat_ch <- chunks_ind$lat_ch[i]
  
  
  
  ## READ DATA -------------------------------------------------------------------------------------
  
  ncs <- 
    cbind(start = c(lon_chunks[[lon_ch]][1], lat_chunks[[lat_ch]][1], 1),
          count = c(lon_chunks[[lon_ch]][2] - lon_chunks[[lon_ch]][1]+1,
                    lat_chunks[[lat_ch]][2] - lat_chunks[[lat_ch]][1]+1,
                    NA))
  
  
  s <- 
    dir_raw_data %>% 
    list.files(full.names = T) %>% 
    
    future_map(function(f){
      
      time_dim <- 
        f %>% 
        read_ncdf(proxy = T) %>% 
        suppressMessages() %>% 
        st_get_dimension_values("time") %>% 
        as.character()
      
      fixed_time <- 
        fn_dates(time_dim, calendar_type)
      
      # import and fix time dim
      str_c(f) %>% 
        read_ncdf(ncsub = ncs) %>% 
        suppressMessages() %>% 
        st_set_dimensions("time", values = fixed_time) -> s 
        
      # regularize grid
      s <-
        s %>%
        st_set_dimensions("lon", values = st_get_dimension_values(s, "lon") %>% round(1)) %>% 
        st_set_dimensions("lat", values = st_get_dimension_values(s, "lat") %>% round(1)) %>% 
        st_set_crs(4326)
      
    },
    .options = furrr_options(seed = NULL)
    ) %>% 
    do.call(c, .)
  
  
  
