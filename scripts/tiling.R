

# TILING

# size of tile (approximate number of cells in each dim)
sz <- 50


# obtain chunks of longitude

d_lon <- 
  s_proxy %>% 
  dim() %>% 
  .[1] %>% 
  seq_len()

n_lon <- 
  round(length(d_lon)/sz)

lon_chunks <- 
  split(d_lon, 
        ceiling(seq_along(d_lon)/(length(d_lon)/n_lon))) %>% 
  map(~c(first(.x), last(.x)))


# obtain chunks of latitude

d_lat <- 
  s_proxy %>% 
  dim() %>% 
  .[2] %>% 
  seq_len()

n_lat <- 
  round(length(d_lat)/sz)

lat_chunks <- 
  split(d_lat, 
        ceiling(seq_along(d_lat)/(length(d_lat)/n_lat))) %>% 
  map(~c(first(.x), last(.x)))


# remove objects
rm(sz, d_lon, n_lon, d_lat, n_lat)
