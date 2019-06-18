library(futile.logger)

Distinct <- function(x) length(unique(x))
Total <- function(x) sum(x)
Count <- function(x) length(x)
Mean <- function(x) mean(x)
Median <- function(x) median(x)
Min <- function(x) min(x)
Max <- function(x) max(x)
Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}
getnullcnt<- function(x)sum(is.na(x))
getnulltable <- function(x) sapply(x,getnullcnt)
decile <- function(x){quantile(x,probs = c(seq(0.1,0.9,by = 0.1),.99),na.rm = T)}

#haversign distance
earth.dist <- function (long1, lat1, long2, lat2){
  rad <- pi/180
  a1 <- lat1 * rad
  a2 <- long1 * rad
  b1 <- lat2 * rad
  b2 <- long2 * rad
  dlon <- b2 - a2
  dlat <- b1 - a1
  a <- (sin(dlat/2))^2 + cos(a1) * cos(b1) * (sin(dlon/2))^2
  c <- 2 * atan2(sqrt(a), sqrt(1 - a))
  R <- 6378.145
  d <- R * c
  return(d)
}

#distribution of a single vector
make_distribution <- function(df,level_vec,distr_var,breakupvector){
  remove_col <- setdiff(colnames(df),level_vec)
  library(tidyr)
  test <- unite(df,col = "level_f",sep = "@",-remove_col)
  decile <- function(x){quantile(x,probs = breakupvector,na.rm = T)}
  test_decile_S <- sapply(unique(test$level_f), function(x){
    library(dplyr)
    a <- test %>% 
      filter(level_f == x)%>%
      select(distr_var)
    decile(a[[1]])
  })
  percentile <- row.names(test_decile_S)
  test_decile_S <- data.frame(t(test_decile_S))
  colnames(test_decile_S) <- percentile
  test_decile_S <- cbind(level = row.names(test_decile_S),test_decile_S)
  library(tidyr)
  test_decile_S <- separate(test_decile_S,level,level_vec,sep = "@")
  test_decile_S}

# Extaracting data from .sql file or a sql query
loadQueryData <- function(con,queryName){
  library("RPostgreSQL")
  library("readr")
  drv <- dbDriver("PostgreSQL")
  if (grepl(".sql",queryName) == 1) 
  {queryData <- dbGetQuery(con, read_file(queryName))
  dbDisconnect(con) 
  return(queryData)}
  else {queryData <- dbGetQuery(con,queryName) 
  dbDisconnect(con) 
  return(queryData)}
}


#Checking the max date of the data frame against particular date
recency_check <- function(db,db_dt_var_strng_frmt,check_dt){
v <- list(as.character(db_dt_var_strng_frmt))
library(dplyr)
library(lubridate)
db2 <- db %>% select_(.dots = v)%>%unique()
names(db2) <- c("dt")
if(as.Date(db2$dt)){
  if(max(db2$dt) == check_dt){db}
  else{print(paste0("dataframe ",deparse(substitute(db)), " not updated"))}
}
}

get_varaible_summary_numeric <- function(db){
  num_var <- sapply(db, is.numeric)
  db_num <- db[,num_var]
  a  <- data.frame(variables = names(db_num),varaince = round(sapply(db_num,var),2),
                   sd =round(sapply(db_num,var),2) ^(1/2))
  b <- round(t(sapply(db_num, summary)),2)
  a <- cbind(a,b)
  final <- data.frame(sapply(a,function(x) {
    if(is.numeric(x)){round(x,2)}
    else{as.character(x)}}))
}

paste_column <- function(df,column_list,col_name) {
  library(tidyr)
  a <- df[,column_list]
  b <- unite(a,col = id ,sep = "",remove = T)
  b$final <- gsub("NA","",b$id)
  c <- cbind.data.frame(df,b$final)
  colnames(c) <- c(colnames(df),col_name)
  c
}

dbupload <- function(con_read,con_write,df,table){
  library(RPostgreSQL)
  library(futile.logger)
  library(gsubfn)
  drv <- dbDriver("PostgreSQL")
  
  append_flag <- TRUE
  overwrite_flag <- FALSE 
  if(dbExistsTable(con_write,table) == T)
  { 
    flog.info("Reading existing data")    
    old_data <- dbReadTable(con_read,table)
    flog.info("Reading existing data done") 
    max_date <- max(old_data$created_at,na.rm = T)
    max_date_df <- max(df$created_at,na.rm =T)
    if(max_date == max_date_df)
    {
      flog.info("No updation required")
      dbDisconnect(con_read)
      dbDisconnect(con_write)
    }
    else
    {
      df_out <- df %>% filter(created_at > max_date)
      flog.info(fn$identity("df_out total rows `nrow(df_out)`"))
      if(nrow(df_out) > 0)
      {
        dbWriteTable(conn = con_write,
                     name = table,
                     value = df_out,
                     append = append_flag,
                     row.names = FALSE,
                     overwrite = overwrite_flag,
                     field.types = table_data_structure)
        dbDisconnect(con_read)
        dbDisconnect(con_write)
        flog.info(fn$identity("table updated till `as.character(max_date_df)`"))
      }
    }
  }
  else{
    flog.info("Writing the table for the first time")
    max_date_df <- max(df$created_at,na.rm =T)
    df_out <- df
    append_flag <- FALSE
    overwrite_flag <- TRUE
    
    if(nrow(df_out) > 0)
    {
      dbWriteTable(conn = con_write,
                   name = table,
                   value = df_out,
                   append = append_flag,
                   row.names = FALSE,
                   overwrite = overwrite_flag,
                   field.types = table_data_structure)
      dbDisconnect(con_read)
      dbDisconnect(con_write)
      flog.info(fn$identity("table updated till `as.character(max_date_df)`"))
    }
  }
}



gdrive_csv_uploader <- function(token,csv_loc,upload_name){
  library(googledrive)
  drive_auth(oauth_token = token, 
             service_token = NULL, reset = FALSE,verbose = TRUE)
  drive_rm(upload_name)
  drive_upload(csv_loc,name  = "wow_insti",verbose = TRUE,type = "spreadsheet")
  drive_share(upload_name,role = c("writer"),type = "domain",domain = 'theporter.in')
  flog.info(fn$identity("uploaded csv `upload_name`"))
}
gs_csv_link_creator <- function(token,csv_name,gs_wb_name,gs_sheet_name,gs_cell_loc){
  library(googlesheets)
  library(futile.logger)
  gs_auth(token = token ,verbose = TRUE)
  list <- gs_ls()
  csv_loc <- as.vector(list[list$sheet_title == csv_name,"alternate"])
  gs_loc <- as.vector(list[list$sheet_title == gs_wb_name,"sheet_key"])
  gs_edit_cells(gs_key(gs_loc$sheet_key),ws = gs_sheet_name,input = csv_loc,col_names = T,anchor = gs_cell_loc)
  flog.info(fn$identity("google sheet `gs_sheet_name` updated with csv link of `csv_name` in the gs `gs_wb_name`"))
}

##create a/b group based on numerical and categorical variables
decile_custom <- function(x){quantile(x,probs = c(seq(0,1,by = 0.25)),na.rm = T)}
create_decile <- function(df,variable_with_quotes){
  library(dplyr)
  v <- list(as.character(variable_with_quotes))
  temp_df <- df %>% select_(.dots = v)
  names(temp_df) <- 'variable'
  new_variable_name <- paste0(variable_with_quotes,'_','bucket')
  break_points = c(unique(decile_custom(temp_df$variable)),Inf)
  temp_df[,new_variable_name] <- cut(temp_df$variable,breaks=break_points,include.lowest = T)
  temp_df[,new_variable_name,drop = F]
}
get_buckets_numerical <- function(df,variables_vector){
  temp <- list()
  for(var in variables_vector){
    temp[variables_vector == var] <- create_decile(df,var)
  }
  df_out <- data.frame(do.call(cbind,temp))
  colnames(df_out) <- variables_vector
  df_out
}
get_buckets_categorical <- function(df,variables_vector){ 
  temp <- list()
  for (var in variables_vector)
  {
    library(dplyr)
    v <- list(as.character(var))
    temp_df <- df %>% select_(.dots = v)
    colnames(temp_df) <- 'variable'
    temp_df$variable <- as.factor(temp_df$variable)
    temp[variables_vector == var] <- temp_df
  }
  df_out <- data.frame(do.call(cbind,temp))
  colnames(df_out) <- variables_vector
  df_out 
}
create_strata <- function(df,numerical_variables,categorical_variables){
  num_b <- get_buckets_numerical(churned_customer_f,variables_vector = numerical_variables)
  cat_b <- get_buckets_categorical(churned_customer_f,variables_vector = categorical_variables)
  final_b <- cbind(num_b,cat_b)
  final_b$stratum <- as.factor(do.call(paste0, as.data.frame(final_b)))
}

get_ab_groups <- function(df,divide_ratio,numerical_variables,categorical_variables){
  df$stratum <- create_strata(churned_customer_f,numerical_variables,categorical_variables)
  strata_freq <- df %>% group_by(stratum) %>% summarise(Freq = n()) %>% mutate(stratum_flag = ifelse(Freq ==1,'singles','nonsingles'))
  df <- merge(df,strata_freq)
  singles <- df %>% filter(stratum_flag == 'singles')
  #non singles a/b 
  d <-  df %>% filter(stratum_flag == 'nonsingles')
  dsample <- data.frame()
  p = divide_ratio
  system.time(
    for(i in unique(d$stratum)) {
      dsub <- subset(d, d$stratum == i)
      B = ceiling(nrow(dsub) * p)
      dsub <- dsub[sample(1:nrow(dsub), B), ]
      dsample <- rbind(dsample, dsub)
    }
  )
  
  dsample_a <- dsample
  dsample_a$group <- 'A'
  dsample_b_index <- setdiff(row.names(d),row.names(dsample_a))
  dsample_b <- d[dsample_b_index,]
  dsample_b$group  <- 'B'
  
  #singles a/b
  if(nrow(dsample_a)- nrow(dsample_b) < nrow(df %>% filter(stratum_flag == 'singles'))){
    singles_a_index <- sample(1:nrow(singles),round(nrow(singles)/2)- nrow(dsample_a)+ nrow(dsample_b),replace = F)
    singles_b_index <- setdiff(row.names(singles),singels_a_index)
    dsample_a_singles <- singles[singles_a_index,]
    dsample_a_singles$group <- 'A'
    dsample_b_singles <- singles[singles_b_index,]
    dsample_b_singles$group <- 'B'
  } else{
    singles_a_index <- NULL 
    singles_b_index <- row.names(singles)
    dsample_b_singles <- singles[singles_b_index,]
    dsample_b_singles$group <- 'B'
  }
  rbind(dsample_a,dsample_b,dsample_a_singles,dsample_b_singles)
}
############################







