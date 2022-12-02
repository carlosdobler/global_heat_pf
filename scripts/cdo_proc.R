
fn_cdo <- function(var_d){
  
  
  # MAX TEMP
  # *************
  # *************
  
  if(var_d == "days-gec-32C-tasmax"){
    
    set_units(32, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
  
  
    # *************
    
  } else if(var_d == "days-gec-35C-tasmax"){
    
    set_units(35, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
  } else if(var_d == "days-gec-38C-tasmax"){
    
    set_units(38, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
  } else if(var_d == "ten-hottest-days-tasmax"){
    
    dir_temp <- "/mnt/pers_disk/dir_temp"
    dir.create(dir_temp)
    
    
    # sort time; extract top 10; calculate mean
    dir_raw_data %>%
      list.files() %>%
      future_walk(function(fff){
        
        str_glue("cdo timsort {dir_raw_data}/{fff} {dir_temp}/sort_{fff}") %>%
          system(ignore.stdout = T, ignore.stderr = T)
        
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
    
    if(file.exists(str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc"))){
      file.remove(str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc"))
    }
    
    dir_temp %>%
      list.files(full.names = T) %>%
      str_subset("/mean_") %>%
      str_flatten(" ") %>%
      
      {system(str_glue("cdo cat {.} {dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc"),
              ignore.stdout = T, ignore.stderr = T)}
    
    # delete intermediate files
    unlink(dir_temp, recursive = T)
    
    
    # *************
    
  } else if(var_d == "mean-tasmax"){
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo yearmean {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
  } else if(var_d == "days-lec-0C-tasmax"){
    
    set_units(0, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -lec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # MIN TEMP
    # *************
    # *************
  
  
  } else if(var_d == "days-gec-20C-tasmin"){
    
    set_units(20, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
        
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
      
    # *************
    
  } else if(var_d == "days-gec-25C-tasmin"){
    
    set_units(25, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
  } else if(var_d == "mean-tasmin"){
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo yearmean {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
  } else if(var_d == "days-lec-0C-tasmin"){
    
    set_units(0, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -lec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
  
    
    
    # WETBULB
    # *************
    # *************
    
    
  } else if(var_d == "days-gec-26C-wetbulb"){
    
    lim_k <- 26
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
  } else if(var_d == "days-gec-28C-wetbulb"){
    
    lim_k <- 28
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
  } else if(var_d == "days-gec-30C-wetbulb"){
    
    lim_k <- 30
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
  
  } else if(var_d == "days-gec-32C-wetbulb"){
    
    lim_k <- 32
    
    outfile <-
      str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
  } else if(var_d == "ten-hottest-days-wetbulb"){
    
    dir_temp <- "/mnt/pers_disk/dir_temp"
    dir.create(dir_temp)
    

    # sort time; extract top 10; calculate mean
    dir_raw_data %>%
      list.files() %>%
      future_walk(function(fff){

        str_glue("cdo timsort {dir_raw_data}/{fff} {dir_temp}/sort_{fff}") %>%
          system(ignore.stdout = T, ignore.stderr = T)

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
    
    if(file.exists(str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc"))){
      file.remove(str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc"))
    }
      
    dir_temp %>%
      list.files(full.names = T) %>%
      str_subset("/mean_") %>%
      str_flatten(" ") %>%

      {system(str_glue("cdo cat {.} {dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc"),
              ignore.stdout = T, ignore.stderr = T)}

    # delete intermediate files
    unlink(dir_temp, recursive = T)
    
    
    
  }
  
}
