
fn_dates <- function(d, cal_type){
  
  if(cal_type == 360){
    
    fixed_time <- 
      
      tibble(time = str_sub(d, end = 10),
           yr = str_sub(time, end = 4),
           mon = str_sub(time, start = 6, end = 7)) %>% 
      
      group_by(yr, mon) %>% 
      nest() %>% 
      mutate(dy = map(data, function(df){
        
        seq(1,
            days_in_month(df$time[1]),
            length.out = 30) %>% 
          round()
        
      })) %>% 
      unnest(c(dy, data)) %>% 
      mutate(time = str_glue("{yr}-{mon}-{dy}") %>% as_date()) %>% 
      pull(time)
    
  } else if(cal_type == 365){
    
    fixed_time <- 
      
      tibble(time = str_sub(d, end = 10),
             yr = str_sub(time, end = 4)) %>% 
      
      group_by(yr) %>% 
      nest() %>% 
      mutate(time = map(data, function(df){
        
        seq(df$time %>% first() %>% as_date(),
            df$time %>% last() %>% as_date(),
            length.out = 365)
        
      })) %>% 
      unnest(time) %>% 
      pull(time)
      
    
  } else if(cal_type == 365.25){
    
    fixed_time <- 
      seq(as_date(first(d)), as_date(last(d)), by = "1 day")
    
  }
  
  return(fixed_time)
  
}



# *****************



fn_statistics <- function(ts){
  
  if(any(is.na(ts))){
    
    c(mean = NA,
      perc05 = NA, 
      perc50 = NA,
      perc95 = NA)
    
  } else {
    
    c(mean = mean(ts),
      quantile(ts, c(0.05, 0.5, 0.95)) %>% setNames(c("perc05", "perc50", "perc95")))
    
  }
}



# *****************




fn_write_nc_wtime_wvars <- function(star_to_export, file_name){
  
  # define dimensions
  dim_lon <- ncdf4::ncdim_def(name = "lon", 
                              units = "degrees_east", 
                              vals = star_to_export %>% 
                                st_get_dimension_values(1) %>% 
                                round(1))
  
  dim_lat <- ncdf4::ncdim_def(name = "lat", 
                              units = "degrees_north", 
                              vals = star_to_export %>% 
                                st_get_dimension_values(2) %>% 
                                round(1))
  
  dim_time <- ncdf4::ncdim_def(name = "warming_levels", 
                               units = "", 
                               vals = star_to_export %>% 
                                 st_get_dimension_values(3) %>% 
                                 as.double()
                               )
  
  # define variables
  varis <- 
    names(star_to_export) %>% 
    map(~ncdf4::ncvar_def(name = .x,
                          units = "",
                          dim = list(dim_lon, dim_lat, dim_time), 
                          missval = -999999))
  
  
  # create file
  ncnew <- ncdf4::nc_create(filename = file_name, 
                            vars = varis,
                            force_v4 = TRUE)
  
  # write data
  seq_along(names(star_to_export)) %>% 
    walk(~ncdf4::ncvar_put(nc = ncnew, 
                           varid = varis[[.x]], 
                           vals = star_to_export %>% select(.x) %>% pull(1)))
  
  ncdf4::nc_close(ncnew)
  
}
