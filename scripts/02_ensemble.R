



# DOMAIN LOOP --------------------------------------------------------------------------------------


for(dom in doms){

  print(str_glue(" "))
  print(str_glue("PROCESSING {dom}"))
  
  
  # LOOP THROUGH VARS
  for(derived_vars_ in derived_vars){
    
    print(str_glue(" "))
    print(str_glue("Processing {derived_vars_}"))
    
    change_import <- 
      tb_vars %>% 
      filter(var_derived == derived_vars_) %>% 
      pull(change_import)
    
    ff <- 
      dir_derived %>% 
      list.files() %>% 
      str_subset(dom) %>% 
      str_subset(derived_vars_)
    
    
    l_s <- 
      
      future_map(ff, function(f){
        
      
        if(change_import == "fix_date_convert_C"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
        } else if(change_import == "convert_C"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
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
                              ymd() %>%
                              year()
                            ) %>%

        # s %>%
        #   st_set_dimensions("time",
        #                     values = seq(1970, length.out = dim(s)[3])
        #   ) %>%
        drop_units()
      })
    
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
    
    
    
    
    # LOOP THROUGH WL
    
    l_s_wl <- 
      
      map(wl, function(wl_){
        
        # wl_ <- "3.0"
        
        print(str_glue("Processing WL {wl_}"))
        
        # SLICE WL
        
        l_s_wl <-
          
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
            
          })
        
        # CALCULATE STATS
        
        l_s_wl %>%
          {do.call(c, c(., along = "time"))} %>%

          st_apply(c(1,2),
                   fn_statistics,
                   FUTURE = F,
                   .fname = "stats") %>%
          aperm(c(2,3,1)) %>%
          split("stats")
        
      })
    
    
    s_result <- 
      l_s_wl %>% 
      {do.call(c, c(., along = "wl"))} %>% 
      st_set_dimensions(3, values = wl)
    
    
    # SAVE
    
    print(str_glue("Saving result"))
    
    res_filename <- 
      str_glue(
        "{dir_bucket_mine}/results/global_heat_pf/02_ensembled/{dom}_{derived_vars_}_ensemble.nc"
      )
    
    if(file.exists(res_filename)){
      file.remove(res_filename)
      print(str_glue("\t (previous file deleted)"))
    }
    
    fn_write_nc_wtime_wvars(
      s_result,
      res_filename
    )
    
  }
  
}



# map_dfr(doms, function(dom){
#   
#   ff <- 
#     dir_derived %>% 
#     list.files() %>% 
#     str_subset(dom) %>% #str_subset("wetbulb") %>% .[1] -> f
#     str_subset(derived_vars_)
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

