

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
    
    gcm_ <- tb_models$gcm[i]
    rcm_ <- tb_models$rcm[i]
    
    
    
    ## DOWNLOAD RAW DATA --------------------------------------------------------------------------
    
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
    
    
    
    ## CDO PRE-PROCESS ----------------------------------------------------------------------------
    
    print(str_glue("Processing with CDO [{rcm_}] [{gcm_}]"))
    
    tb_files %>% 
      filter(gcm == gcm_,
             rcm == rcm_) %>% 
      
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
          
          # print(t_i)
          
          
        }
        
        
      })
    
    
    dir_raw_data %>% 
      list.files(full.names = T) %>% 
      str_subset("yrsplit") %>% 
      walk(file.remove)

    
    ff <-
      dir_raw_data %>%
      list.files(full.names = T) %>%
      str_flatten(" ")
    
    dir_cat <- "/mnt/pers_disk/cat"
    dir.create(dir_cat)
    
    system(str_glue("cdo cat {ff} {dir_cat}/cat.nc"),
           ignore.stdout = T, ignore.stderr = T)
    
    
    # future_
    walk(derived_vars, function(derived_vars_){
      
      print(str_glue("   {derived_vars_}"))
      
      fn_cdo(derived_vars_)
      
      
    }#,
    #.options = furrr_options(seed = NULL))
    )
    
    unlink(dir_cat, recursive = T)
    
    ## DELETE RAW FILES ---------------------------------------------------------------------------
    
    unlink(dir_raw_data, recursive = T)
    
  }
  
}

