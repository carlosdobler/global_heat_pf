
var_d <- derived_vars_


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
  
  
  
  
  # MEAN TEMP
  # *************
  # *************
  
} else if(var_d == "mean-tasmean"){
  
  outfile <-
    str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
  
  str_glue("cdo yearmean {dir_cat}/cat.nc {outfile}") %>%
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
  
  
  
  # PRECIPITATION
  # *************
  # *************
  
  
} else if(var_d == "total-precip"){
  
  outfile <-
    str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
  
  str_glue("cdo yearsum {dir_cat}/cat.nc {outfile}") %>%
    system(ignore.stdout = T, ignore.stderr = T)
  
  
  # *************
  
  
} else if(var_d == "ninety-wettest-days"){
  
  
  fn_90_wettest <- function(x){
    
    if(any(is.na(x))){
      
      pr <- rep(NA, length(all_years))
      
    } else {
      
      # running sum of 90 days
      # value assigned to last obs of the window
      runsum <- 
        x %>% 
        slider::slide_dbl(sum, .before = 89, .complete = T, .step = 2)
      
      # initialize vector
      pr <- rep(NA_real_, length(all_years))
      
      dy <- 0 # first iteration; no day
      
      # loop through years
      for(i in 1:length(all_years)){
        
        time_range <- which(time_dim == all_years[i]) %>% range()
        
        # if max is within the last 90 days of the year
        # shorten the range of time to look for max
        # so that windows do not overlap
        if(i != 1 & dy >= time_range[1]-90){
          
          dy <- which.max(runsum[(dy+90):time_range[2]])+(dy+90)-1
          
        } else {
          
          dy <- which.max(runsum[time_range[1]:time_range[2]])+time_range[1]-1
          
        }
        
        pr[i] <- runsum[dy]
        
      }
    }
    
    return(pr)
    
  }
  
  
  outfile <-
    str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
  
  s_proxy <- 
    str_glue("{dir_cat}/cat.nc") %>% 
    read_ncdf(proxy = T) %>% 
    suppressMessages()
  
  source("scripts/tiling.R")
  
  time_dim <- 
    s_proxy %>% 
    st_get_dimension_values("time") %>% 
    year()
  
  all_years <- time_dim %>% unique()
  
  dir_tmp <- "/mnt/pers_disk/tmp"
  dir.create(dir_tmp)
  
  iwalk(lon_chunks, function(lon_, i_lon){
    iwalk(lat_chunks, function(lat_, i_lat){
      
      print(str_glue("      processing chunk {i_lon} - {i_lat}"))
      
      s_proxy[,lon_[1]:lon_[2], lat_[1]:lat_[2],] %>% 
        st_apply(c(1,2),
                 fn_90_wettest,
                 FUTURE = T,
                 .fname = "time") %>% 
        aperm(c(2,3,1)) %>% 
        st_as_stars(proxy = F) -> foo
      
      write_stars(foo, str_glue("{dir_tmp}/{dom}_tmpfile_{i_lon}_{i_lat}.tif"))
      
    })
  })
  
  
  foo <- 
    map(seq_along(lat_chunks), function(i_lat){
      tibble(
        file = dir_tmp %>% 
          list.files(full.names = T) %>% 
          str_subset(str_glue("_{i_lat}.tif"))) %>% 
        mutate(col = str_extract(file, "_[:digit:]*_"),
               col = parse_number(col)) %>% 
        arrange(col) %>% 
        pull(file) %>%
        
        read_stars(along = 1)
    })
  
  bar <- 
    foo %>% 
    {do.call(c, c(., along = 2))}
  
  fn_write_nc_derived(pull(bar, 1),
                      outfile,
                      st_get_dimension_values(s_proxy, "lon"),
                      st_get_dimension_values(s_proxy, "lat"),
                      str_glue("{all_years}0101") %>% as_date() %>% as.integer(),
                      "pr",
                      "kg/m^2/s")
  
  unlink(dir_tmp, recursive = T)
  
  
  # *************
  
  
} else if(var_d == "p98-precip"){
  
  outfile <-
    str_glue("{dir_derived}/{dom}_{var_d}_yr_{rcm_}_{gcm_}.nc")
  
  # str_glue("cdo yearmax {dir_cat}/cat.nc {outfile}") %>%
  #   system(ignore.stdout = T, ignore.stderr = T)
  
  str_glue("cdo yearpctl,98 {dir_cat}/cat.nc -yearmin {dir_cat}/cat.nc -yearmax {dir_cat}/cat.nc {outfile}") %>% 
    system(ignore.stdout = T, ignore.stderr = T)
  
  
}



