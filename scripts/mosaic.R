
source("scripts/setup.R")
source("scripts/functions.R")

plan(multicore)


tb_dom <- 
  tibble(
    dom = c("NAM", "CAM", "SAM", "EUR", "AFR", "WAS", "CAS", "SEA", "EAS", "AUS"),
    centr_lon = c(263.0-360, 287.29-360, 299.70-360, 9.75, 17.60, 67.18, 74.64, 118.04, 116.57, 147.63),
    centr_lat = c(47.28, 10.20, -21.11, 49.68, -1.32, 16.93, 47.82, 6.5, 34.40, -24.26)
  )



derived_vars <- c("days-gec-32C",
                  "days-gec-35C",
                  "days-gec-38C",
                  "ten-hottest-days",
                  "mean-tasmax")





l_s <-
  map(set_names(tb_dom$dom), function(dom){
    
    s <- 
      str_glue("{dir_bucket_mine}/results/global_heat_pf/02_ensembled") %>%
      list.files(full.names = T) %>%
      str_subset(dom) %>%
      str_subset(derived_vars[1]) %>%
      read_ncdf(var = "mean", ncsub = cbind(start = c(1,1,1), count = c(NA, NA, 1))) %>%
      suppressMessages() %>% 
      adrop()
    
    if(dom == "EAS"){
      
      s <- 
        s %>% 
        filter(lon < 180)
      
    } else if(dom == "AUS"){
      
      s1 <- 
        s %>% 
        filter(lon < 180)
      
      s2 <- 
        s %>% 
        filter(lon > 180)
      
      s2 <- 
        st_set_dimensions(s2, 
                          which = "lon", 
                          values = st_get_dimension_values(s2, 
                                                           "lon", 
                                                           center = F)-360) %>% 
        st_set_crs(4326)
      
      s <- 
        list(s1, s2) %>% 
        map(as, "SpatRaster") %>% 
        do.call(terra::merge, .) %>% #plot()
        st_as_stars(proxy = F)
      
      s <- 
        s %>% 
        setNames("mean") %>% 
        st_set_dimensions(c(1,2), names = c("lon", "lat"))
      
      rm(s1, s2)
      
    }
    
    return(s)
    
    
  })


l_s_valid <- 
  l_s %>% 
  map(function(s){
    
    s %>% 
      setNames("v") %>% 
      mutate(v = ifelse(is.na(v), NA, 1))
    
  })


global <- 
  c(
    st_point(c(-179.9, -89.9)),
    st_point(c(179.9, 89.9))
  ) %>% 
  st_bbox() %>% 
  st_set_crs(4326) %>% 
  st_as_stars(dx = 0.2, values = NA) %>%  
  st_set_dimensions(c(1,2), names = c("lon", "lat"))



l_s_dist <-
  pmap(tb_dom, function(dom, centr_lon, centr_lat){
    
    s_valid <-
      l_s_valid %>%
      pluck(dom)
    
    pt_valid <-
      s_valid %>%
      st_as_sf(as_points = T)
    
    # centr_lon <- tb_dom[tb_dom$dom == dom,]$centr_lon
    # centr_lat <- tb_dom[tb_dom$dom == dom,]$centr_lat
    
    centr <-
      st_point(c(centr_lon, centr_lat)) %>%
      st_sfc() %>%
      st_set_crs(4326)
    
    # radius <- 
    #   dim(s_valid) %>% 
    #   max() %>% 
    #   {./2} %>% 
    #   round()
    
    s_dist <-
      pt_valid %>%
      mutate(dist = st_distance(., centr),
             dist = set_units(dist, NULL),
             # dist = ifelse(dist > radius*20000, radius*20000, dist),
             # dist = 1/dist^0.1,
             # dist = 1/log(dist),
             dist = max(dist)-dist,
             # dist = 1/log(dist),
             # dist = 1/dist,
             dist = scales::rescale(dist)
      ) %>%
      select(dist) %>%
      st_rasterize(global)
    # st_rasterize(s_valid)
    
    
    # s_dist %>%
    #   slice(x, 369) %>%
    #   as_tibble() %>%
    #   pull(dist) %>% #range(na.rm = T)
    #   .[!is.na(.)] %>%
    #   # .[. < 0.2] %>%
    #   plot()
    
    return(s_dist)
    
  }) %>%
  set_names(tb_dom$dom)


l_s_dist %>% 
  do.call(c, .) %>% 
  merge() %>% 
  st_apply(c(1,2), function(foo){
    
    bar <- ifelse(is.na(foo), 0, 1)
    
    if(sum(bar) > 1){
      sum(foo^2, na.rm = T)
    } else {
      NA
    }
    
  }, 
  FUTURE = T,
  .fname = "sum_intersect") -> s_intersections


l_s_weights <- 
  map(l_s_dist, function(s){
    
    c(s, s_intersections) %>% 
      
      mutate(sum_intersect = ifelse(sum_intersect == 0, 1e-10, sum_intersect)) %>% 
      mutate(weights = ifelse(is.na(sum_intersect) & !is.na(dist), 1, dist^2/sum_intersect)) %>%
      select(weights)
    
  })






# LOOP THROUGH WL AND VARS

# derived_vars_ <- derived_vars[2]
# wl_ <- "2.0"
# wl_pos <- 4


wl <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")

walk(derived_vars, function(derived_vars_){
  
  print(derived_vars_)
  
  l_mos_wl <- 
    
    imap(wl, function(wl_, wl_pos){
      
      print(wl_)
      
      l_s_weighted <- 
        
        map(tb_dom$dom, function(dom){
          
          s <- 
            str_glue("{dir_bucket_mine}/results/global_heat_pf/02_ensembled") %>%
            list.files(full.names = T) %>%
            str_subset(dom) %>%
            str_subset(derived_vars_) %>%
            read_ncdf %>%
            suppressMessages()
          
          if(dom == "EAS"){
            
            s <- 
              s %>% 
              filter(lon < 180)
            
          } else if(dom == "AUS"){
            
            s1 <- 
              s %>% 
              filter(lon < 180)
            
            s2 <- 
              s %>% 
              filter(lon > 180)
            
            s2 <- 
              st_set_dimensions(s2, 
                                which = "lon", 
                                values = st_get_dimension_values(s2, 
                                                                 "lon", 
                                                                 center = F)-360) %>% 
              st_set_crs(4326)
            
            s <- 
              map(names(s), function(v){
                
                list(
                  s1 %>% select(v),
                  s2 %>% select(v)
                ) %>% 
                  map(as, "SpatRaster") %>% 
                  do.call(terra::merge, .) %>% #plot()
                  st_as_stars(proxy = F) %>% 
                  setNames(v) %>% 
                  st_set_dimensions(c(1,2,3), names = c("lon", "lat", "warming_levels")) %>% 
                  st_set_dimensions(3, values = st_get_dimension_values(s, "warming_levels"))
                
              }) %>% 
              do.call(c, .)
            
          }
          
          
          r <- 
            s %>% 
            st_warp(l_s_weights[[1]]) %>% 
            slice(warming_levels, wl_pos)
          
          c(r,
            l_s_weights %>% pluck(dom)) %>% 
            
            mutate(mean = mean*weights,
                   perc05 = perc05*weights,
                   perc50 = perc50*weights,
                   perc95 = perc95*weights) %>% 
            select(-weights)
          
        }) %>% 
        set_names(tb_dom$dom)
      
      
      l_s_weighted %>%
        map(merge, name = "stats") %>%
        imap(~setNames(.x, .y)) %>%
        unname() %>% 
        do.call(c, .) %>% 
        merge(name = "doms") %>%
        
        st_apply(c(1,2,3), function(foo){
          
          if(all(is.na(foo))){
            NA
          } else {
            sum(foo, na.rm = T)
          }
          
        },
        FUTURE = T) %>% 
        setNames(wl_) -> mos
      
      return(mos)
      
    })
  
  
  s <- 
    l_mos_wl %>% 
    do.call(c, .) %>% 
    merge(name = "warming_level") %>% 
    split("stats")
  
  
  fn_write_nc_wtime_wvars(s, 
                          str_glue("/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/{derived_vars_}_v01.nc"))
  
  
  
})
















wl <- 1
v <- "mean"

s %>% 
  select(v) %>% 
  slice(warming_level, wl) %>% 
  as_tibble() %>% 
  rename("v" = 3) %>% 
  ggplot(aes(x,y,fill = v)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
  coord_equal() +
  theme(axis.title = element_blank())

# NAM
s %>% 
  select(v) %>% 
  slice(warming_level, wl) %>% 
  as_tibble() %>% 
  rename("v" = 3) %>% 
  ggplot(aes(x,y,fill = v)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
  coord_equal(xlim = c(-130, -70), ylim = c(10,55)) +
  theme(axis.title = element_blank())

# SAM
s %>% 
  select(v) %>% 
  slice(warming_level, wl) %>%   
  as_tibble() %>% 
  rename("v" = 3) %>% 
  ggplot(aes(x,y,fill = v)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
  coord_equal(xlim = c(-100, -30), ylim = c(-30,25)) +
  theme(axis.title = element_blank())

# AFR
s %>% 
  select(v) %>% 
  slice(warming_level, wl) %>% 
  as_tibble() %>% 
  rename("v" = 3) %>% 
  ggplot(aes(x,y,fill = v)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
  coord_equal(xlim = c(-20, 55), ylim = c(0,50)) +
  theme(axis.title = element_blank())

# CAS
mos %>% 
  slice(stats, 1) %>% 
  as_tibble() %>% 
  rename("v" = 3) %>% 
  ggplot(aes(x,y,fill = v)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
  coord_equal(xlim = c(25, 85), ylim = c(15,70)) +
  theme(axis.title = element_blank())

# AUS
mos %>% 
  slice(stats, 1) %>% 
  as_tibble() %>% 
  rename("v" = 3) %>% 
  ggplot(aes(x,y,fill = v)) +
  geom_raster() +
  colorspace::scale_fill_continuous_sequential("Plasma", na.value = "transparent", rev = F) +
  coord_equal(xlim = c(90, 170), ylim = c(-50,10)) +
  theme(axis.title = element_blank())
