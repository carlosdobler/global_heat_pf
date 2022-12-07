

# DOMAIN LOOP -------------------------------------------------------------------------------------


for(dom in doms){
  
  print(str_glue(" "))
  print(str_glue("PROCESSING {dom}"))
  
  # DATA TABLE ------------------------------------------------------------------------------------
  
  tb_files <- 
    fn_data_table(var_input)
  
  
  # MODELS TABLE ----------------------------------------------------------------------------------
  
  tb_models <-
    fn_models_table(tb_files)
  
  
  # LOOP THROUGH MODELS ---------------------------------------------------------------------------
  
  
  for(i in seq_len(nrow(tb_models))){
    
    print(str_glue(" "))
    print(str_glue("PROCESSING model {i} / {nrow(tb_models)}"))
    
    gcm_ <- tb_models$gcm[i]
    rcm_ <- tb_models$rcm[i]
    
    
    ## DOWNLOAD RAW DATA --------------------------------------------------------------------------
    
    print(str_glue("Downloading raw data [{rcm_}] [{gcm_}]"))
    
    dir_raw_data <- str_glue("{dir_pers_disk}/raw_data")
    
    if(dir.exists(dir_raw_data)){
      print(str_glue("   (previous dir_raw_data deleted)"))
      unlink(dir_raw_data, recursive = T)
    } 
    
    dir.create(dir_raw_data)
    
    tb_files_mod <- 
      tb_files %>% 
      filter(gcm == gcm_,
             rcm == rcm_) 
      
    tb_files_mod %>% 
      future_pwalk(function(loc, file, ...){
        
        loc_ <-
          loc %>% 
          str_replace("/mnt/bucket_cmip5", "gs://cmip5_data")
        
        str_glue("{loc_}/{file}") %>%
          {system(str_glue("gsutil cp {.} {dir_raw_data}"), 
                  ignore.stdout = T, ignore.stderr = T)}
        
      })
    
    print(str_glue("   Done: {nrow(tb_files_mod)} files downloaded"))
    
    
    ## CDO PRE-PROCESS ----------------------------------------------------------------------------
    
    print(str_glue("Processing with CDO [{rcm_}] [{gcm_}]"))
    
    tb_files_mod %>%
      future_pwalk(function(file, t_i, t_f, ...){
        
        yr_i <- year(as_date(t_i))
        yr_f <- year(as_date(t_f))
        
        f <- str_glue("{dir_raw_data}/{file}")
        
        
        
        if(str_detect(var_input, "wetbulb")){
          
          f_new <- str_glue("{dir_raw_data}/yrfix_{yr_i}.nc")
          
          system(str_glue("cdo -a setdate,{yr_i}-01-01 {f} {f_new}"),
                 ignore.stdout = T, ignore.stderr = T)
          
          file.remove(f)
          
          
        } else {
          
          system(str_glue("cdo splityear {f} {dir_raw_data}/yrsplit_"),
                 ignore.stdout = T, ignore.stderr = T)
          
          dir_raw_data %>% 
            list.files(full.names = T) %>% 
            str_subset("yrsplit") %>%
            str_subset(str_flatten(yr_i:yr_f, "|")) %>% 
            
            walk2(seq(yr_i, yr_f), function(f2, yr){
              
              f_new <- str_glue("{dir_raw_data}/yrfix_{yr}.nc")
              
              system(str_glue("cdo -a setdate,{yr}-01-01 {f2} {f_new}"),
                     ignore.stdout = T, ignore.stderr = T)
              
              file.remove(f2)
              
            })
          
          file.remove(f)
          
        }
        
        
      })
    
    
    bad_remnants <- 
      dir_raw_data %>% 
      list.files(full.names = T) %>% 
      str_subset("yrsplit")
    
    if(length(bad_remnants) > 0){
      print(str_glue("   ({length(bad_remnants)} bad files - deleted)"))
      
      bad_remnants %>% 
        walk(file.remove)
    }
    
    
    ff <-
      dir_raw_data %>%
      list.files(full.names = T) %>%
      str_flatten(" ")
    
    dir_cat <- "/mnt/pers_disk/cat"
    
    if(dir.exists(dir_cat)){
      print(str_glue("   (previous dir_cat deleted)"))
      unlink(dir_cat, recursive = T)
    }
    
    dir.create(dir_cat)
    
    system(str_glue("cdo cat {ff} {dir_cat}/cat.nc"),
           ignore.stdout = T, ignore.stderr = T)
    
    
    # future_
    walk(derived_vars, function(derived_vars_){
      
      print(str_glue("   {derived_vars_}"))
      
      old_file <- 
        dir_derived %>% 
        list.files(full.names = T) %>% 
        str_subset(dom) %>% 
        str_subset(derived_vars_) %>% 
        str_subset(gcm_) %>% 
        str_subset(rcm_)
      
      if(length(old_file) > 0){
        file.remove(old_file)
        print(str_glue("      (replacing: {length(old_file)} file)"))
      }
      
      fn_cdo(derived_vars_)
      
      time_steps <- 
        str_glue("{dir_derived}/{dom}_{derived_vars_}_yr_{rcm_}_{gcm_}.nc") %>% 
        read_ncdf(proxy = T, make_time = F) %>%
        suppressMessages() %>% 
        suppressWarnings() %>% 
        st_get_dimension_values("time") %>% 
        length()
      
      print(str_glue("      Done: new file with {time_steps} timesteps"))
      
    })
    
    unlink(dir_cat, recursive = T)
    
    ## DELETE RAW FILES ---------------------------------------------------------------------------
    
    unlink(dir_raw_data, recursive = T)
    
  }
  
}

