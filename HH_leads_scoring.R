
connect_to_db <- function(source) {
  drv <- dbDriver("PostgreSQL")
  source("~/Documents/connections.R")
  flog.info(fn$identity("Connecting to `source`"))
  if (source == "oms") {
    connection <- dbConnect(
      drv,
      host = oms_host,
      port = oms_port,
      user = oms_user,
      password = oms_pwd,
      dbname = oms_db
    )
  } else if (source == "sfms") {
    connection <- dbConnect(
      drv,
      host = sf_host,
      port = sf_port,
      user = sf_user,
      password = sf_pwd,
      dbname = sf_db
    )
  }else if (source == "redshift") {
    connection <- dbConnect(
      drv,
      host = redshift_host,
      port = redshift_port,
      user = redshift_user,
      password = redshift_pwd,
      dbname = redshift_db
    )
  }
  return(connection)
}

run_query <- function(source, query_statement) {
  # Runs a Query Statement on the connection source
  # Output: Query Data
  # Define Connection from Source
  connection <- connect_to_db(source)
  # Get Query Data
  query_data <- dbGetQuery(
    conn = connection,
    statement = query_statement
  )
  # Disconnect Connection
  dbDisconnect(conn = connection)
  # Return Query Data

  return(query_data)
}



library("RPostgreSQL")
library("gsubfn")
library("futile.logger")
library("readr")
library("dplyr")
library("lubridate")
library("googlesheets4")
library("data.table")


fetch_hh_leads_details <- function(start_date, end_date) {
  query <- fn$identity(read_file("~/Documents/queries/hh_leads.sql"))
  hh_leads_details <- run_query(source = "sfms", query_statement = query)
  hh_leads_with_customer_id <- hh_leads_details %>%
    filter(!is.na(customer_id)) %>%
    distinct(customer_id, .keep_all = TRUE) %>%
    mutate(whether_converted = ifelse(difftime( third_order_date, first_order_date,units = "days")<= 30 , 1,0 ))
  return(hh_leads_with_customer_id)
}
##test =  ifelse(difftime( hh_leads$third_order_date, hh_leads$first_order_date,units = "days") <= 30 , 1 ,0 )
hh_leads <- fetch_hh_leads_details("2021-05-31", "2021-06-01")
##hh_leads <- hh_leads %>% filter(whether_converted != 1)

##fetch app installation details

fetch_installation_time_details <- function(hh_leads) {
  phone_number_list <- paste("'", paste(hh_leads$phone_number, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/app_installation_detail.sql"))
  get_installation_details <- run_query(source = "sfms", query_statement = query)
  return(get_installation_details)
}


fetch_leads_app_installation_detail<- function(hh_leads) {
  installation_time <- fetch_installation_time_details(hh_leads)
  lead_with_installation_time<- hh_leads %>% left_join(installation_time) %>% mutate(first_order_after_app_install = difftime( as.Date(first_order_date), as.Date(app_install_time),units = "days") )
  return(lead_with_installation_time)
}

app_install_detail = fetch_leads_app_installation_detail(hh_leads)

## customer frequency
fetch_customer_frequency <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/customer_frequency.sql"))
  orders <- run_query(source = "oms", query_statement = query)

  return(orders)
}

lead_with_frequency <- function(app_install_detail) {
  frequency <- fetch_customer_frequency(hh_leads)
  lead_with_frequency<- app_install_detail %>% left_join(frequency)
  return(lead_with_frequency)
}

frequency = lead_with_frequency(app_install_detail)


## email verification
fetch_email_verification_status <- function(hh_leads){
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/email_verification.sql"))
  email_verification <- run_query(source = 'oms',query_statement = query)

  return(email_verification)
}

fetch_email_verification_status_hh_leads <- function(frequency) {
  email <- fetch_email_verification_status(hh_leads)
  leads_with_email_status <- frequency %>% left_join(email)
  leads_email_verification_status <- leads_with_email_status %>% mutate(whether_verified = case_when((confirmed_at <= first_order_date) ~ "verified", TRUE ~ "not_verified"))
  return(leads_email_verification_status)
}

email_verification <- fetch_email_verification_status_hh_leads(frequency)


##customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
##email <- fetch_email_verification_status(hh_leads)


## porter gold

fetch_gold_subscription <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/porter_gold.sql"))
  gold_customers <- run_query(source = "oms", query_statement = query)

  return(gold_customers)
}

fetch_gold_details_by_hh_leads <- function(email_verification) {
  gold_subscription <- fetch_gold_subscription(hh_leads)
  total_gold_bought <- email_verification %>%
    left_join(gold_subscription) %>%
    filter(gold_subscribed_at <= as.Date(first_order_date) + 15) %>%
    group_by(customer_id) %>%
    summarise(total_gold_subs = n())
  hh_leads_with_gold <- email_verification %>% left_join(total_gold_bought)
  whether_bought_gold <- hh_leads_with_gold %>% mutate(whether_bought_gold = case_when(is.na(total_gold_subs) ~ "not bought", TRUE ~ "bought"))
  return(whether_bought_gold)
}

porter_gold <- fetch_gold_details_by_hh_leads(email_verification)

## wallet recharge

fetch_wallet_recharge_details <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/wallet_recharge.sql"))
  wallet_recharge_details <- run_query(source = "oms", query_statement = query)
  return(wallet_recharge_details)
}

fetch_hh_leads_wallet_recharge_details <- function(porter_gold) {
  wallet_recharge <- fetch_wallet_recharge_details(hh_leads)
  # get total number of recharges before entering hh
  total_recharges <- porter_gold %>%
    left_join(wallet_recharge) %>%
    filter(recharged_at <= as.Date(first_order_date) + 15) %>%
    group_by(customer_id) %>%
    summarise(recharges = n())
  # defining wallet recharge as boolean asthese are hh leads ,they wont do it more than once or twice
  wallet_recharge_hh_leads <- porter_gold %>% left_join(total_recharges, by = "customer_id")
  whether_recharged_wallet <- wallet_recharge_hh_leads %>% mutate(whether_recharged = case_when(is.na(recharges) ~ "not_recharged", TRUE ~ "recharged"))
  return(whether_recharged_wallet)
}

wallet_recharge = fetch_hh_leads_wallet_recharge_details(porter_gold)
## competitor apps

fetch_competitor_app <- function(hh_leads){
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/competitor_apps.sql"))
  competitor_app <- run_query(source = 'redshift',query_statement = query)
  return(competitor_app)
}

fetch_app_data_at_lead_level <- function(hh_leads_with_features, app_detail) {
  customers_with_app <- app_detail %>%
    dplyr::filter(is_installed == "TRUE") %>%
    distinct(customer_id, .keep_all = TRUE)
  customers_without_app <- app_detail %>%
    dplyr::filter(is_installed == "FALSE") %>%
    distinct(customer_id, .keep_all = TRUE)
  # get installed and not installed information per customer in order to select unique customers
  leads_with_comp_app_installed_details <- rbind(customers_with_app, customers_without_app)
  # select leads with atleast one competitor app as installed
  get_app_detail_at_lead_level <- leads_with_comp_app_installed_details %>% distinct(customer_id, .keep_all = TRUE)
  hh_leads_with_comp_app_detail <- hh_leads_with_features %>%
    left_join(get_app_detail_at_lead_level) %>%
    mutate(whether_installed = case_when(is.na(is_installed) ~ NA_character_, is_installed == "TRUE" ~ "installed", TRUE ~ "not_installed"))
  return(hh_leads_with_comp_app_detail)
}

fetch_competitors_app_hh_leads <- function(wallet_recharge) {
  competitors_app <- fetch_competitor_app(hh_leads)
  app_data_at_lead_level <- fetch_app_data_at_lead_level(wallet_recharge, competitors_app) %>%
    # rename whether installed in order to pass it as factor for competitor app installation
    rename(whether_installed_competitor = whether_installed) %>%
    # deselect is_installed as all app metrics has this column and mentioning join columns will be cumbersome
    select(-is_installed)
  return(app_data_at_lead_level)
}

competitor_app <- fetch_competitors_app_hh_leads(wallet_recharge)

## total sessions and time spent

fetch_hh_leads_total_sessions_and_time_spent <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/total_sessions.sql"))
  time_spent_per_booking <- run_query(source = "redshift", query_statement = query)

  return(time_spent_per_booking)
}

get_total_sessions <- function(competitor_app) {
  session_details <- fetch_hh_leads_total_sessions_and_time_spent(hh_leads)
  # filter sessions created before entering hh
  total_sessions_per_customer <- competitor_app %>%
    left_join(session_details) %>%
    filter(booking_start_time <= as.Date(first_order_date) + 15) %>%
    # calculate total sessions per customer
    group_by(customer_id) %>%
    summarise(total_sessions_per_customer = n())
  total_session_per_hh_leads <- competitor_app %>%
    left_join(total_sessions_per_customer) %>%
    mutate_at("total_sessions_per_customer", ~ replace(., is.na(.), 0))
  # bucketing total sessions created based on median session (require reviewer's input)
  define_number_of_session <- total_session_per_hh_leads %>% mutate(number_of_session = case_when(total_sessions_per_customer == 0 ~ "0 sessions", total_sessions_per_customer <= median(total_sessions_per_customer) ~ "less_than_median_sessions", TRUE ~ "more_than_median_sessions"))
  return(define_number_of_session)
}

total_sessions <- get_total_sessions(competitor_app)

get_time_spent_per_session <- function(total_sessions) {
  lead_with_time_spent_per_session <- fetch_hh_leads_total_sessions_and_time_spent(hh_leads)
  total_time_spend_on_bookings <- total_sessions %>%
    left_join(lead_with_time_spent_per_session) %>%
    filter(booking_start_time <= as.Date(first_order_date) + 15) %>%
    group_by(customer_id) %>%
    summarise(total_time_spent = sum(time_spent))
  sessions <- total_sessions %>% left_join(total_time_spend_on_bookings, by = "customer_id")
  time_per_session <- sessions %>% mutate(time_spent_per_session = total_time_spent / total_sessions_per_customer)
  bucketing_time_spent_per_session <- time_per_session %>%
    mutate_at("time_spent_per_session", ~ replace(., is.na(.), 0)) %>%
    mutate(decile = ntile(time_spent_per_session, 10)) %>%
    mutate(time_spent_bucket = case_when(
      decile <= 5 ~ "less_tha_median", decile == 6 ~ "50-60", decile == 7 ~ "60-70",
      decile == 8 ~ "70-80", decile == 9 ~ "80-90", decile == 10 ~ "90-10"
    ))
  return(bucketing_time_spent_per_session)
}

total_time_spent <- get_time_spent_per_session(total_sessions)

## customer type


fetch_customer_type <- function(hh_leads){
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/customer_type.sql"))
  customer_type <- run_query(source = 'sfms',query_statement = query)

  return(customer_type)
}

fetch_hh_leads_customer_type <- function(total_time_spent) {
  customers_type <- fetch_customer_type(hh_leads)
  hh_leads_customer_type <- total_time_spent %>%
    left_join(customers_type) %>%
    distinct(customer_id, .keep_all = TRUE)
  return(hh_leads_customer_type)
}
##customers_type <- fetch_customer_type(hh_leads)
hh_customer_type <- fetch_hh_leads_customer_type(total_time_spent)


## email,industry,customer type__

fetch_customers_email_industry_type_industry <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/email_and_industry_type.sql"))
  customers_email_industry_type_industry <- run_query(source = "oms", query_statement = query)

  return(customers_email_industry_type_industry)
}

fetch_customers_details <- function(hh_customer_type) {
  customers_details <- fetch_customers_email_industry_type_industry(hh_leads)
  leads_with_email_ind_type_details <- hh_customer_type %>% left_join(customers_details)
  # making list of probable personal emails
  personal_email_list <- data.frame(email = c("gmail", "yahoo", "outlook"))
  # define email category for all leads
  email_category <- leads_with_email_ind_type_details %>% mutate(email_type = case_when((grepl(paste(personal_email_list$email, collapse = "|"), leads_with_email_ind_type_details$email)) ~ "personal_email", is.na(email) ~ NA_character_, TRUE ~ "business_email"))
  return(email_category)
}


hh_email_industry_type = fetch_customers_details(hh_customer_type)

## business app
fetch_business_app <- function(hh_leads){
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/business_app.sql"))
  business_app <- run_query(source = 'redshift',query_statement = query)
  return(business_app)
}

fetch_business_app_hh_leads <- function(hh_email_industry_type) {
  business_app <- fetch_business_app(hh_leads)
  app_data_at_lead_level <- fetch_app_data_at_lead_level(hh_email_industry_type, business_app) %>%
    # rename whether installed in order to pass it as factor for business app installation
    rename(whether_installed_business = whether_installed) %>%
    # deselect is_installed as all app metrics has this column and mentioning join columns will be     cumbersome
    select(-is_installed)
  return(app_data_at_lead_level)
}

hh_business_app <- fetch_business_app_hh_leads(hh_email_industry_type)


## payment app
fetch_payment_app <- function(hh_leads){
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/payment_app.sql"))
  payment_app <- run_query(source = 'redshift',query_statement = query)
  return(payment_app)
}

fetch_payment_app_hh_leads <- function(hh_business_app) {
  payment_app <- fetch_payment_app(hh_leads)
  app_data_at_lead_level <- fetch_app_data_at_lead_level(hh_business_app,
                                                         payment_app) %>%
    # rename whether installed in order to pass it as factor for payment app installation
    rename(whether_installed_payment = whether_installed) %>%
    # deselect is_installed as all app metrics has this column and mentioning join columns will be     cumbersome
    select(-is_installed)
  return(app_data_at_lead_level)
}

hh_payment_app <- fetch_payment_app_hh_leads(hh_business_app)


## order tracked

fetch_hh_leads_track_screen_clicks <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/order_track.sql"))
  tracking_detail <- run_query(source = "redshift", query_statement = query)
  return(tracking_detail)
}


fetch_leads_track_order_clicks <- function(hh_payment_app) {
  track_screen_clicks <- fetch_hh_leads_track_screen_clicks(hh_leads)
  # get total track live screen click per customer
  track_clicks <- hh_payment_app %>%
    left_join(track_screen_clicks) %>%
    filter(as.Date(event_timestamp) <= as.Date(first_order_date)) %>%
    group_by(customer_id) %>%
    summarise(total_clicks = n())
  hh_leads_with_total_track_clicks <- hh_payment_app %>%
    left_join(track_clicks) %>%
    mutate_at("total_clicks", ~ replace(., is.na(.), 0))
  # bucket clicks based on median
  clicks_bucket <- hh_leads_with_total_track_clicks %>% mutate(total_clicks_bucket = case_when(total_clicks == 0 ~ "never_clicked", total_clicks <= median(total_clicks) ~ "less_than_median_clicks", TRUE ~ "more_than_median_clicks"))
  return(clicks_bucket)
}

order_tracked <- fetch_leads_track_order_clicks(hh_payment_app)

## cancelled orders
fetch_cancelled_order_time <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/Cancelled_orders.sql"))
  cancellation_time <- run_query(source = "oms", query_statement = query)

  return(cancellation_time)
}

fetch_cancelled_orders <- function(order_tracked) {
  # fetch leads cancelled order time
  leads_with_cancellation_time <- fetch_cancelled_order_time(hh_leads)
  leads_with_cancelled_order <- order_tracked %>% left_join(leads_with_cancellation_time)
  # filter cases where it was cancelled before hh
  when_cancelled <- leads_with_cancelled_order %>%
    mutate(whether_cancelled = ifelse((as.Date(trip_cancelled_time) >= as.Date(first_order_date)) &  (as.Date(trip_cancelled_time) <= as.Date(first_order_date) + 15) ,"cancelled","never_cancelled")) %>% 
    filter(whether_cancelled == "cancelled")
  # calculate total cancelled orders
  total_cancel <- when_cancelled %>%
    group_by(customer_id) %>%
    summarise(total_can_orders = n())
  # replace nas with 0
  leads_with_cancellation <- order_tracked %>%
    left_join(total_cancel) %>%
    mutate_at("total_can_orders", ~ replace(., is.na(.), 0))
  # define whether cancelled
  define_cancellations <- leads_with_cancellation %>% mutate(whether_cancelled = case_when(total_can_orders == 0 ~ "not_cancelled", TRUE ~ "cancelled"))
  return(define_cancellations)
}

cancelled_orders <- fetch_cancelled_orders(order_tracked)


# as.Date(order_tracked$first_order_date,"%d/%m/%y")
# test =   cancelled_orders %>% mutate(first_order_time = as.Date(first_order_date),first_order_time_1 = first_order_time + 15,second_order_time = as.Date(second_order_date))
# test$third_order_time[is.na(test$third_order_time)] = "2099-12-31"
# test_1 = test %>% mutate(test_1 = ifelse((first_order_time < trip_cancelled_time & trip_cancelled_time< third_order_time) ,1,
#                                                             0))
# test_2 = test %>% mutate(diff = difftime(second_order_time,first_order_time,units = "days"))
# 
# 
# test_1 = test %>% mutate(test_1 = ifelse((as.Date(trip_cancelled_time) > first_order_time)&  (as.Date(trip_cancelled_time) < first_order_time + 15) ,1,0))
# 

## mobile device
  fetch_mobile_device <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/mobile_type.sql"))
  mobile_data <- run_query(source = "redshift", query_statement = query)

  return(mobile_data)
}

fetch_customers_mobile <- function(cancelled_orders) {
  mobile_data <- fetch_mobile_device(hh_leads)
  leads_with_mobile_type <- cancelled_orders %>% left_join(mobile_data)
 return(leads_with_mobile_type)
}

mobile_device_data<-fetch_customers_mobile(cancelled_orders)

# # vehicle category
fetch_vehicle_type <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/vehicle_type.sql"))
  vehicle_data <- run_query(source = "oms", query_statement = query)

  return(vehicle_data)
}

fetch_customers_vehicle <- function(mobile_device_data) {
  vehicle_data <- fetch_vehicle_type(hh_leads)
  leads_with_vehicle_type <- mobile_device_data %>% left_join(vehicle_data)
  return(leads_with_vehicle_type)
}

first_order_vehicle_type <- fetch_customers_vehicle(mobile_device_data)

# trip rating
fetch_trip_rating <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/trip_rating.sql"))
  ratings_data <- run_query(source = "oms", query_statement = query)

  return(ratings_data)
}

fetch_hh_trip_rating <- function(first_order_vehicle_type) {
  ratings_data <- fetch_trip_rating(hh_leads)
  leads_with_trip_rating <- first_order_vehicle_type %>% left_join(ratings_data)
  return(leads_with_trip_rating)
}

first_order_ratings_data = fetch_hh_trip_rating(first_order_vehicle_type)

first_order_ratings_data$device_type = ifelse(first_order_ratings_data$device_model %ilike% "Iphone" |
                                                first_order_ratings_data$device_model %ilike% "SM-G9" |
                                                first_order_ratings_data$device_model %ilike% "Pixel" |
                                                first_order_ratings_data$device_model %ilike% "SM-N9"|
                                                first_order_ratings_data$device_model %ilike% "ONEPLUS"|
                                                first_order_ratings_data$device_model %ilike% "Asus_I" |
                                                first_order_ratings_data$device_model %ilike% "Asus_Z" |
                                                first_order_ratings_data$device_model %ilike% "Redmi K20"|
                                                first_order_ratings_data$device_model %ilike% "LG-H"
                                              ,"Premium",
                                              ifelse(is.na(first_order_ratings_data$device_model) == TRUE, "NA" ,"Non Premium"))


# leads source
fetch_leads_source<- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/leads_source.sql"))
  leads_source_data <- run_query(source = "sfms", query_statement = query)
  
  return(leads_source_data)
}

fetch_hh_leads_source<- function(first_order_ratings_data) {
  leads_source_data <- fetch_leads_source(hh_leads)
  leads_with_source <- first_order_ratings_data %>% left_join(leads_source_data)
  return(leads_with_source)
}

hh_leads_source_data <- fetch_hh_leads_source(first_order_ratings_data)

# ## order tracked calls

fetch_hh_leads_calls_to_partner <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/call_event_to_partner.sql"))
  call_detail <- run_query(source = "redshift", query_statement = query)
  return(call_detail)
}


fetch_leads_calls_to_partner <- function(hh_leads_source_data) {
  call_partner_clicks <- fetch_hh_leads_calls_to_partner(hh_leads)
  # get total track live screen click per customer
  call_clicks <- hh_leads_source_data %>%
    left_join(call_partner_clicks) %>%
    filter(as.Date(event_timestamp) <= as.Date(first_order_date)) %>%
    group_by(customer_id) %>%
    summarise(total_calls = n())
  hh_leads_with_total_calls <- hh_leads_source_data %>%
    left_join(call_clicks) %>%
    mutate_at("total_calls", ~ replace(., is.na(.), 0))
  # bucket clicks based on median
  clicks_bucket <- hh_leads_with_total_calls %>% mutate(total_calls_bucket = case_when(total_clicks == 0 ~ "never_clicked", total_calls <= median(total_calls) ~ "less_than_median_clicks", TRUE ~ "more_than_median_clicks"))
  return(clicks_bucket)
}

hh_leads_calls <- fetch_leads_calls_to_partner(hh_leads_source_data)

# table(hh_leads_calls$whether_converted,hh_leads_calls$total_calls_bucket)
# prop.table(table(hh_leads_calls[["whether_converted"]], hh_leads_calls[["total_calls_bucket"]]), margin = 2) * 100

## order related CC tickets

fetch_order_CC_details <- function(hh_leads) {
  phone_number_list <- paste("'", paste(hh_leads$phone_number, collapse = "','"), "'", sep = "")
  order_list <- paste("'", paste(hh_leads_calls$order_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/order_related_cc_ticket.sql"))
  get_order_related_cc_detail <- run_query(source = "sfms", query_statement = query)
  return(get_order_related_cc_detail)
}

# get_order_related_cc_detail = fetch_order_CC_details(hh_leads)
# get_order_related_cc_detail$customer_partner_phone = as.numeric(get_order_related_cc_detail$customer_partner_phone )

fetch_leads_order_CC_details <- function(hh_leads_calls) {
  get_hh_order_related_cc_detail <- fetch_order_CC_details(hh_leads)
 #get_hh_order_related_cc_detail$customer_partner_phone = as.numeric(get_hh_order_related_cc_detail$customer_partner_phone )
  # get total track live screen click per customer
  order_CC <- hh_leads_calls %>%
    left_join(get_hh_order_related_cc_detail, by = c("phone_number" = "customer_partner_phone")) %>%
    filter(!is.na(order_stage_v2)) %>%
    group_by(phone_number) %>%
    summarise(total_order_CC = n())
  hh_leads_with_total_order_CC <- hh_leads_calls %>%
    left_join(order_CC) %>%
    mutate_at("total_order_CC", ~ replace(., is.na(.), 0))
  # bucket clicks based on median
   return(hh_leads_with_total_order_CC)
}

hh_leads_with_total_order_CC = fetch_leads_order_CC_details(hh_leads_calls)

hh_leads_with_total_order_CC$total_order_CC = ifelse(hh_leads_with_total_order_CC$total_order_CC >= 1 , 1, 0)
# summary(hh_leads_IMEI_number_with_distinct_customers$total_order_CC )
## non order related CC ticket

fetch_non_order_CC_details <- function(hh_leads) {
  phone_number_list <- paste("'", paste(hh_leads$phone_number, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/non_order_related_cc_ticket.sql"))
  get_non_order_related_cc_detail <- run_query(source = "sfms", query_statement = query)
  return(get_non_order_related_cc_detail)
}

fetch_leads_non_order_CC_details <- function(hh_leads_with_total_order_CC) {
  get_hh_non_order_related_cc_detail <- fetch_non_order_CC_details(hh_leads)
  get_hh_non_order_related_cc_detail$customer_partner_phone = as.numeric(get_hh_non_order_related_cc_detail$customer_partner_phone )
  # get total track live screen click per customer
 non_order_CC <- hh_leads_with_total_order_CC %>%
    left_join(get_hh_non_order_related_cc_detail, by = c("phone_number" = "customer_partner_phone")) %>%
   filter(as.Date(created_at) <= as.Date(first_order_date) + 15) %>%
    group_by(phone_number) %>%
    summarise(total_non_order_CC = n())
  hh_leads_with_total_non_order_CC <- hh_leads_with_total_order_CC %>%
    left_join(non_order_CC) %>%
    mutate_at("total_non_order_CC", ~ replace(., is.na(.), 0))
  # bucket clicks based on median
  return(hh_leads_with_total_non_order_CC)
}
hh_leads_with_total_non_order_CC = fetch_leads_non_order_CC_details(hh_leads_with_total_order_CC)


# ## accepted but refuse to go
fetch_driver_accepted_but_refused_to_go <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/driver_accepted_but_refused_to_go.sql"))
  accepted_but_refuse_to_go <- run_query(source = "oms", query_statement = query)
  
  return(accepted_but_refuse_to_go)
}

fetch_driver_accepted_but_refused_to_go_orders <- function(hh_leads_with_total_order_CC) {
  # fetch leads cancelled order time
  leads_with_cancellation_time <- fetch_driver_accepted_but_refused_to_go(hh_leads)
  leads_with_cancelled_order <- hh_leads_with_total_order_CC %>% left_join(leads_with_cancellation_time, by ='customer_id')
  # filter cases where it was cancelled before hh
  when_cancelled <- leads_with_cancelled_order %>%
    mutate(whether_cancelled = ifelse((as.Date(trip_cancelled_time) >= as.Date(first_order_date)) &  (as.Date(trip_cancelled_time) <= as.Date(first_order_date) + 15) ,"cancelled","never_cancelled")) %>% 
    filter(whether_cancelled == "cancelled")
  # calculate total cancelled orders
  total_cancel <- when_cancelled %>%
    group_by(customer_id) %>%
    summarise(total_accepted_but_refuse_to_go_orders = n())
  # replace nas with 0
  leads_with_cancellation <- hh_leads_with_total_order_CC %>%
    left_join(total_cancel, by ='customer_id') %>%
    mutate_at("total_accepted_but_refuse_to_go_orders", ~ replace(., is.na(.), 0))
  # define whether cancelled
  define_cancellations <- leads_with_cancellation %>% mutate(whether_accepted_but_refuse_to_go_orders = case_when(total_accepted_but_refuse_to_go_orders == 0 ~ "No", TRUE ~ "Yes"))
  return(define_cancellations)
}

accepted_but_refused_to_go_orders <- fetch_driver_accepted_but_refused_to_go_orders(hh_leads_with_total_order_CC)

## order_delayed

fetch_order_delayed <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  orders_list <- paste("'", paste(hh_leads_calls$order_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/order_delayed.sql"))
  get_order_delayed_detail <- run_query(source = "oms", query_statement = query)
  return(get_order_delayed_detail)
}

fetch_leads_order_delayed <- function(accepted_but_refused_to_go_orders) {
  get_hh_order_delayed <- fetch_order_delayed(hh_leads)
  order_delayed <- accepted_but_refused_to_go_orders %>%
    left_join(get_hh_order_delayed) %>%
  # bucket clicks based on median
  return(order_delayed)
}

hh_leads_with_order_delayed = fetch_leads_order_delayed(accepted_but_refused_to_go_orders)


#IMEI number

fetch_leads_IMEI_number <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/IMEI_number.sql"))
  leads_IMEI_number <- run_query(source = "redshift", query_statement = query)
  
  return(leads_IMEI_number)
}

fetch_hh_leads_IMEI_number<- function(hh_leads_with_order_delayed) {
  leads_IMEI_number <- fetch_leads_IMEI_number(hh_leads)
  leads_with_IMEI_number <- hh_leads_with_order_delayed %>% left_join(leads_IMEI_number, by = 'customer_id')
  return(leads_with_IMEI_number)
}

hh_leads_IMEI_number = fetch_hh_leads_IMEI_number(hh_leads_with_order_delayed)

fetch_distinct_customers_per_IMEI = function(hh_leads) {
  IMEI_list = paste("'", paste(hh_leads_IMEI_number$a_device_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/IMEI_distinct_customers.sql"))
  leads_IMEI_number <- run_query(source = "redshift", query_statement = query)
  return(leads_IMEI_number)
}

fetch_hh_distinct_customers_per_IMEI <- function(hh_leads_IMEI_number) {
  leads_IMEI_number <- fetch_distinct_customers_per_IMEI(hh_leads)
  leads_with_IMEI_number_with_distinct_customers <- hh_leads_IMEI_number %>% left_join(leads_IMEI_number, by = 'a_device_id')
  return(leads_with_IMEI_number_with_distinct_customers)
}

hh_leads_IMEI_number_with_distinct_customers = fetch_hh_distinct_customers_per_IMEI(hh_leads_IMEI_number)

hh_leads_IMEI_number_with_distinct_customers$distinct_customer_id = ifelse(hh_leads_IMEI_number_with_distinct_customers$distinct_customer_id > 1, 'Multiple',
                                                                           ifelse(hh_leads_IMEI_number_with_distinct_customers$distinct_customer_id == 1, 'Single','NA'))


#hh_leads_IMEI_number_with_distinct_customers = hh_leads_IMEI_number
table(hh_leads_IMEI_number_with_distinct_customers$whether_converted,hh_leads_IMEI_number_with_distinct_customers$vehicle_type)
prop.table(table(hh_leads_IMEI_number_with_distinct_customers[["whether_converted"]], hh_leads_IMEI_number_with_distinct_customers[["vehicle_type"]]), margin = 2) * 100

table(hh_leads_IMEI_number_with_distinct_customers$whether_converted,hh_leads_IMEI_number_with_distinct_customers$first_form_source)
prop.table(table(hh_leads_IMEI_number_with_distinct_customers[["whether_converted"]], hh_leads_IMEI_number_with_distinct_customers[["first_form_source"]]), margin = 2) * 100


table(hh_leads_IMEI_number_with_distinct_customers$whether_converted,hh_leads_IMEI_number_with_distinct_customers$whether_cancelled)

prop.table(table(hh_leads_IMEI_number_with_distinct_customers[["whether_converted"]], hh_leads_IMEI_number_with_distinct_customers[["whether_cancelled"]]), margin = 2) * 100

table(hh_leads_source_data$whether_converted,hh_leads_source_data$lead_source)

prop.table(table(hh_leads_source_data[["whether_converted"]], hh_leads_source_data[["lead_source"]]), margin = 2) * 100

table(hh_leads_IMEI_number_with_distinct_customers$whether_converted,hh_leads_IMEI_number_with_distinct_customers$distinct_customer_id)

prop.table(table(hh_leads_IMEI_number_with_distinct_customers[["whether_converted"]], hh_leads_IMEI_number_with_distinct_customers[["distinct_customer_id"]]), margin = 2) * 100

table(hh_leads_IMEI_number_with_distinct_customers$whether_converted,hh_leads_IMEI_number_with_distinct_customers$whether_accepted_but_refuse_to_go_orders)

prop.table(table(hh_leads_IMEI_number_with_distinct_customers[["whether_converted"]], hh_leads_IMEI_number_with_distinct_customers[["whether_accepted_but_refuse_to_go_orders"]]), margin = 2) * 100


colSums(is.na(hh_leads_source_data))

table(hh_leads_IMEI_number_with_distinct_customers$whether_converted,hh_leads_IMEI_number_with_distinct_customers$total_order_CC)

prop.table(table(hh_leads_IMEI_number_with_distinct_customers[["whether_converted"]], hh_leads_IMEI_number_with_distinct_customers[["total_order_CC"]]), margin = 2) * 100

table(hh_leads_IMEI_number_with_distinct_customers$whether_converted,hh_leads_IMEI_number_with_distinct_customers$total_calls_bucket)

prop.table(table(hh_leads_IMEI_number_with_distinct_customers[["whether_converted"]], hh_leads_IMEI_number_with_distinct_customers[["total_calls_bucket"]]), margin = 2) * 100


# test = cancelled_orders %>% ifelse(first_order_date <= canc)
#   
# order_tracked_testing$first_order_date_1 <- as.Date(order_tracked_testing$first_order_date,"%d/%m/%y")
# order_tracked_testing <- order_tracked_testing %>% filter(first_order_date_1 <= "2021-04-05")
# class(order_tracked_testing$first_order_date)
# #order_tracked_testing_test_backup <- order_tracked_testing_test
# # order_tracked_testing_test <- order_tracked_testing_test_backup
# class(order_tracked_testing_testing$"30_days_flag")
# order_tracked_testing_test <- order_tracked_testing_test %>% rename("X30_days_flag" = "30_days_flag")
# order_tracked_testing_test <- order_tracked_testing_testing %>% filter(flag ==1)
# 
# write.csv(order_tracked_testing, "~/Documents/queries/hh_leads_with_features_v2.csv")
# 
# write.csv(order_tracked_testing_testing, "~/Documents/queries/hh_leads_with_features_testing.csv")
# write.csv(hh_leads_IMEI_number_with_distinct_customers, "~/Documents/queries/hh_leads_with_features_v6.csv")



# reading from csv file

hh_leads_source_data<- read.csv("~/Documents/queries/hh_leads_with_features_v5.csv")

#hh_leads<- read.csv("~/Documents/queries/hh_leads_with_features_v5.csv")
#order_tracked_testing_backup <- order_tracked_testing
#order_tracked= order_tracked_backup

#order_tracked_testing_test_backup <- order_tracked_testing_test
#order_tracked_testing_test = order_tracked_testing_test_backup

#order_tracked_testing_testing_backup <- order_tracked_testing_testing
#order_tracked= order_tracked_testing_backup


table(order_tracked$whether_converted,order_tracked$customer_frequency)
table(order_tracked$whether_converted,order_tracked$whether_installed_business)
table(order_tracked$whether_converted,order_tracked$whether_installed_payment)

#hh_customer_type$whether_converted <- ifelse(difftime( hh_leads$third_order_date, hh_leads$first_order_date,units = "days")<= 30 , 1,0 )

#order_tracked_testing$first_order_after_app_install = difftime( as.Date(hh_customer_type$first_order_date), as.Date(hh_customer_type$app_install_time),units = "days")

colSums(is.na(hh_leads_IMEI_number_with_distinct_customers))
# replacing NAs
hh_leads_IMEI_number_with_distinct_customers$whether_converted[is.na(hh_leads_IMEI_number_with_distinct_customers$whether_converted)] = 0
hh_leads_IMEI_number_with_distinct_customers$driver_rating[is.na(hh_leads_IMEI_number_with_distinct_customers$driver_rating)] = 99
hh_leads_IMEI_number_with_distinct_customers$whether_installed_competitor[is.na(hh_leads_IMEI_number_with_distinct_customers$whether_installed_competitor)] = "not_installed"
hh_leads_IMEI_number_with_distinct_customers$whether_installed_business[is.na(hh_leads_IMEI_number_with_distinct_customers$whether_installed_business)] = "not_installed"
hh_leads_IMEI_number_with_distinct_customers$whether_installed_payment[is.na(hh_leads_IMEI_number_with_distinct_customers$whether_installed_payment)] = "not_installed"
hh_leads_IMEI_number_with_distinct_customers$whether_bought_gold[is.na(hh_leads_IMEI_number_with_distinct_customers$whether_bought_gold)] = "not bought"
hh_leads_IMEI_number_with_distinct_customers$whether_recharged[is.na(hh_leads_IMEI_number_with_distinct_customers$whether_recharged)] = "not_recharged"
hh_leads_IMEI_number_with_distinct_customers$whether_verified[is.na(hh_leads_IMEI_number_with_distinct_customers$whether_verified)] = "not_verified"
#order_tracked$customer_frequency[is.na(order_tracked$customer_frequency)] = "NA"
hh_leads_IMEI_number_with_distinct_customers$customer_type[is.na(hh_leads_IMEI_number_with_distinct_customers$customer_type)] = "NA"
hh_leads_IMEI_number_with_distinct_customers$first_order_after_app_install[is.na(hh_leads_IMEI_number_with_distinct_customers$first_order_after_app_install)] = 999999
#order_tracked$app_download_time_of_day[is.na(order_tracked$app_download_time_of_day)] = "NA"
#order_tracked$email_type[is.na(order_tracked$email_type)] = "NA"
hh_leads_IMEI_number_with_distinct_customers$industry_type[is.na(hh_leads_IMEI_number_with_distinct_customers$industry_type)] = "NA"
#order_tracked$industry_type[order_tracked$industry_type == ""] = "NA"
hh_leads_IMEI_number_with_distinct_customers$distinct_customer_id[is.na(hh_leads_IMEI_number_with_distinct_customers$distinct_customer_id)] = "NA"

hh_leads_IMEI_number_with_distinct_customers$device_type[is.na(hh_leads_IMEI_number_with_distinct_customers$device_type)] = "NA"


# removing NAs
hh_leads_IMEI_number_with_distinct_customers = hh_leads_IMEI_number_with_distinct_customers[!is.na(hh_leads_IMEI_number_with_distinct_customers$customer_frequency),]
hh_leads_IMEI_number_with_distinct_customers = hh_leads_IMEI_number_with_distinct_customers[!is.na(hh_leads_IMEI_number_with_distinct_customers$email_type),]
hh_leads_IMEI_number_with_distinct_customers= hh_leads_IMEI_number_with_distinct_customers[!is.na(hh_leads_IMEI_number_with_distinct_customers$app_download_time_of_day),]
hh_leads_IMEI_number_with_distinct_customers= hh_leads_IMEI_number_with_distinct_customers[!is.na(hh_leads_IMEI_number_with_distinct_customers$vehicle_type),]
hh_leads_IMEI_number_with_distinct_customers= hh_leads_IMEI_number_with_distinct_customers[!is.na(hh_leads_IMEI_number_with_distinct_customers$first_form_source),]

summary(hh_leads_IMEI_number_with_distinct_customers$whether_converted)
table(hh_leads_IMEI_number_with_distinct_customers$whether_converted)

summary(hh_leads_IMEI_number_with_distinct_customers$device_type)


hh_leads_IMEI_number_with_distinct_customers$whether_converted = as.factor(hh_leads_IMEI_number_with_distinct_customers$whether_converted)
hh_leads_IMEI_number_with_distinct_customers$customer_type = as.factor(hh_leads_IMEI_number_with_distinct_customers$customer_type)
hh_leads_IMEI_number_with_distinct_customers$time_spent_bucket = as.factor(hh_leads_IMEI_number_with_distinct_customers$time_spent_bucket)
hh_leads_IMEI_number_with_distinct_customers$number_of_session = as.factor(hh_leads_IMEI_number_with_distinct_customers$number_of_session)
hh_leads_IMEI_number_with_distinct_customers$whether_recharged = as.factor(hh_leads_IMEI_number_with_distinct_customers$whether_recharged)
hh_leads_IMEI_number_with_distinct_customers$whether_installed_competitor = as.factor(hh_leads_IMEI_number_with_distinct_customers$whether_installed_competitor)
hh_leads_IMEI_number_with_distinct_customers$whether_bought_gold = as.factor(hh_leads_IMEI_number_with_distinct_customers$whether_bought_gold)
hh_leads_IMEI_number_with_distinct_customers$whether_verified = as.factor(hh_leads_IMEI_number_with_distinct_customers$whether_verified)
hh_leads_IMEI_number_with_distinct_customers$app_download_time_of_day = as.factor(hh_leads_IMEI_number_with_distinct_customers$app_download_time_of_day)
hh_leads_IMEI_number_with_distinct_customers$customer_frequency = as.factor(hh_leads_IMEI_number_with_distinct_customers$customer_frequency)
hh_leads_IMEI_number_with_distinct_customers$whether_installed_business = as.factor(hh_leads_IMEI_number_with_distinct_customers$whether_installed_business)
hh_leads_IMEI_number_with_distinct_customers$whether_installed_payment = as.factor(hh_leads_IMEI_number_with_distinct_customers$whether_installed_payment)
hh_leads_IMEI_number_with_distinct_customers$email_type = as.factor(hh_leads_IMEI_number_with_distinct_customers$email_type)
hh_leads_IMEI_number_with_distinct_customers$industry_type = as.factor(hh_leads_IMEI_number_with_distinct_customers$industry_type)
hh_leads_IMEI_number_with_distinct_customers$total_clicks_bucket = as.factor(hh_leads_IMEI_number_with_distinct_customers$total_clicks_bucket)
hh_leads_IMEI_number_with_distinct_customers$device_type = as.factor(hh_leads_IMEI_number_with_distinct_customers$device_type)
hh_leads_IMEI_number_with_distinct_customers$vehicle_type = as.factor(hh_leads_IMEI_number_with_distinct_customers$vehicle_type)
hh_leads_IMEI_number_with_distinct_customers$driver_rating = as.factor(hh_leads_IMEI_number_with_distinct_customers$driver_rating)
hh_leads_IMEI_number_with_distinct_customers$total_calls_bucket = as.factor(hh_leads_IMEI_number_with_distinct_customers$total_calls_bucket)
hh_leads_IMEI_number_with_distinct_customers$distinct_customer_id = as.factor(hh_leads_IMEI_number_with_distinct_customers$distinct_customer_id)
hh_leads_IMEI_number_with_distinct_customers$whether_accepted_but_refuse_to_go_orders = as.factor(hh_leads_IMEI_number_with_distinct_customers$whether_accepted_but_refuse_to_go_orders)
hh_leads_IMEI_number_with_distinct_customers$total_order_CC = as.factor(hh_leads_IMEI_number_with_distinct_customers$total_order_CC)




hh_leads_IMEI_number_with_distinct_customers$whether_cancelled = as.factor(hh_leads_IMEI_number_with_distinct_customers$whether_cancelled)

hh_leads_IMEI_number_with_distinct_customers$first_form_source = as.factor(hh_leads_IMEI_number_with_distinct_customers$first_form_source)

#,"%d/%m/%y"
hh_leads_IMEI_number_with_distinct_customers$driver_rating_1 = ifelse(hh_leads_IMEI_number_with_distinct_customers$driver_rating == 99 , "Not rated","rated")
hh_leads_IMEI_number_with_distinct_customers$first_and_second_order_difference = difftime(as.Date(hh_leads_IMEI_number_with_distinct_customers$second_order_date),as.Date(hh_leads_IMEI_number_with_distinct_customers$first_order_date), units = "days")
hh_leads_IMEI_number_with_distinct_customers$first_and_second_order_flag = ifelse(is.na(hh_leads_IMEI_number_with_distinct_customers$first_and_second_order_difference)== TRUE, "Not placed",
                                                               ifelse(hh_leads_IMEI_number_with_distinct_customers$first_and_second_order_difference <=10, "Before 10 days", "After 10 days"))

hh_leads_IMEI_number_with_distinct_customers$driver_rating_1 = as.factor(hh_leads_IMEI_number_with_distinct_customers$driver_rating_1)
hh_leads_IMEI_number_with_distinct_customers$first_and_second_order_flag = as.factor(hh_leads_IMEI_number_with_distinct_customers$first_and_second_order_flag)

names(hh_leads_IMEI_number_with_distinct_customers)
View(first_order_ratings_data)

test_AB = hh_leads_IMEI_number_with_distinct_customers %>% filter(whether_converted == 0)

hh_leads_IMEI_number_with_distinct_customers_subset = select(hh_leads_IMEI_number_with_distinct_customers,"id","sf_created_at","lead_spot_id","first_order_date","customer_id",
                                         "phone_number","whether_converted","app_download_time_of_day","first_order_after_app_install","customer_frequency",
                                         "whether_verified","whether_bought_gold","whether_recharged","whether_installed_competitor",
                                         "whether_installed_business","whether_installed_payment","number_of_session",
                                         "time_spent_bucket","customer_type","industry_type","email_type","total_clicks_bucket",
                                         "device_type","vehicle_type","whether_cancelled","driver_rating_1","first_and_second_order_flag","first_form_source","total_calls_bucket",
                                         "total_order_CC","distinct_customer_id","whether_accepted_but_refuse_to_go_orders","total_calls_bucket")

summary(hh_leads_IMEI_number_with_distinct_customers$whether_converted)

# order_tracked_testing_subset <- order_tracked_testing_subset %>% select("customer_id","sf_created_at")
# train_subset <- train_subset %>% select("customer_id","sf_created_at")
# check = merge(x = order_tracked_testing_test_subset,y = train_subset,by.x = "customer_id",by.y="customer_id",all.x = TRUE)
# order_tracked_testing_test = check %>% filter(is.na(as.character(sf_created_at.y)))

# intersect(order_tracked_testing_test_subset,order_tracked_testing_subset)
# order_tracked_testing = order_tracked_testing[,-42]
#install.packages("caret")
library(caret)
set.seed(101)
trainIndex <- createDataPartition(hh_leads_IMEI_number_with_distinct_customers_subset$whether_converted, p = .8,
                                  list = FALSE,
                                  times = 1)

train <- hh_leads_IMEI_number_with_distinct_customers_subset[ trainIndex,]
test <- hh_leads_IMEI_number_with_distinct_customers_subset[-trainIndex,]
#install.packages("ROSE")
# library(ROSE)
# summary(order_tracked_testing_test_subset$whether_converted)
# balanced_train <- SMOTE(train,whether_converted, K =  5,dup_size =  0)
# colSums(is.na(train))

train_converted <-  train %>% filter (whether_converted == 1)
train_not_converted <-  train %>% filter (whether_converted == 0)
train_not_converted <- train_not_converted[sample(nrow(train_not_converted), 50000), ]
 
train_new <- rbind(train_converted, train_not_converted)

# n_legit <- 15388
# new_frac_legit <- 0.82
# n_new <- nrow(train) # = 24600
# fraction_fraud_new <- 0.50
# new_n_total <- n_legit/new_frac_legit # = 189202/0.50 = 42816
# 
# oversampling_result <- ovun.sample(whether_converted ~ .,
#                                    data = order_tracked_testing_test_subset,
#                                    method = "over"
#                                    ,
#                                    N = new_n_total,
#                                    seed = 2018)
# 
# oversampling_result <- ovun.sample(whether_converted ~ .,
#                                    data = train,
#                                    method = "both"
#                                    ,
#                                    N = n_new,
#                                    p = fraction_fraud_new,
#                                    seed = 2018)
# 
# oversampling_result <- SMOTE(X = train[, -7],
#       target = train$whether_converted,
#       K = 5,
#       dup_size = 10)
# 
# oversampled_train <- oversampling_result$data
# summary(oversampled_train$whether_converted)
# summary(oversampled_train)
# 
# ## 75% of the sample size
# smp_size <- floor(0.8 * nrow(order_tracked_testing))

## set the seed to make your partition reproducible
##set.seed(123)
##train_ind <- sample(seq_len(nrow(order_tracked_testing)), size = smp_size)

##train <- order_tracked_testing[ train_ind,]
##test <- order_tracked_testing[-train_ind,]

summary(train_new$whether_converted) 
summary(test$whether_converted) 
summary(hh_leads_IMEI_number_with_distinct_customers_subset$whether_converted) 
#install.packages("ISLR")
library(ISLR)
names(hh_leads_IMEI_number_with_distinct_customers_subset)

glm.fit = glm(whether_converted ~ app_download_time_of_day +  first_order_after_app_install+ customer_frequency + whether_bought_gold  + whether_recharged 
              + whether_installed_competitor  + number_of_session  + email_type  + whether_installed_business 
                + vehicle_type + device_type + whether_cancelled + driver_rating_1 + first_and_second_order_flag + first_form_source+ total_calls_bucket 
              , data = train_new, family = binomial )

saveRDS(glm.fit, "~/Documents/queries/model.rds")

glm_model = readRDS("~/Documents/queries/model.rds")
library(rsq)
rsq(glm.fit)
rsq(glm.fit, adj = TRUE)
summary(glm.fit)

varImp(glm.fit)
anova(glm.fit, test="Chisq")


glm.probs <- predict(glm.fit,type = "response")
glm.probs[1:10]
glm.pred <- ifelse(glm.probs > 0.5, 1, 0)
mean(glm.probs)

attach(train_new)
table =table(train_new$whether_converted,glm.pred)
table
accuracy = mean(glm.pred == whether_converted)
accuracy

precision = table[2,2]/sum(table[,2])
precision

recall= table[2,2]/sum(table[2,])
recall

F_measure = (2 * precision * recall) / (precision + recall)
F_measure

summary(hh_leads_IMEI_number_with_distinct_customers_subset$total_order_CC)
# order_tracked_testing_new <- order_tracked_testing[sample(nrow(order_tracked_testing), 50000), ]
# summary(order_tracked_testing_new$whether_converted)


test_AB$first_order_after_app_install = as.numeric(test_AB$first_order_after_app_install)



glm.probs.test <- predict(glm_model,
                     newdata = test_AB,
                     type = "response")
glm.probs.test[1:10]
glm.pred.test <- ifelse(glm.probs.test > 0.5, 1, 0)

attach(test)

table <- table(test_AB$whether_converted,glm.pred.test)
table

accuracy_Test <- sum(diag(table)) / sum(table)
accuracy_Test

precision = table[2,2]/sum(table[,2])
precision

recall= table[2,2]/sum(table[2,])
recall

F_measure = (2 * precision * recall) / (precision + recall)
F_measure

test_2  = test_2[!is.na(test_2$diff),]
test_2$bucket =  ifelse((test_2$diff >=0 & test_2$diff <= 2), "0 - 2",
                        ifelse((test_2$diff >2 & test_2$diff <= 5), "3 - 5", 
                               ifelse((test_2$diff >5 & test_2$diff <= 10), "6 - 10", 
                                      ifelse((test_2$diff >10 & test_2$diff <= 15), "11 - 15", 
                                             ifelse((test_2$diff >15 & test_2$diff <= 20), "16 - 20", 
                                                    ifelse((test_2$diff >20 & test_2$diff <= 30), "21 - 30", 
                                                           ifelse((test_2$diff >30), "> 30","" )
                                                                         ))))))
test_3 = test_2 %>% group_by(bucket,whether_converted) %>% summarise(total = n())

plot_1 = ggplot(test_3, aes(x = bucket, y = total))+
  geom_col(aes(fill = whether_converted), width = 0.5)
plot_1

test_pred <- data.frame(test_AB,glm.probs.test)


mean(test_pred$glm.probs.test)

test_pred %>% group_by(whether_converted) %>% summarise(mean = mean(glm.probs.test))


test_pred$prob <- ifelse((test_pred$glm.probs.test >=0 & test_pred$glm.probs.test <= 0.1), "0 - 0.1",
                         ifelse((test_pred$glm.probs.test >0.1 & test_pred$glm.probs.test <= 0.2), "0.1 - 0.2", 
                         ifelse((test_pred$glm.probs.test >0.2 & test_pred$glm.probs.test <= 0.3), "0.2 - 0.3", 
                         ifelse((test_pred$glm.probs.test >0.3 & test_pred$glm.probs.test <= 0.4), "0.3 - 0.4", 
                         ifelse((test_pred$glm.probs.test >0.4 & test_pred$glm.probs.test <= 0.5), "0.4 - 0.5", 
                         ifelse((test_pred$glm.probs.test >0.5 & test_pred$glm.probs.test <= 0.6), "0.5 - 0.6", 
                         ifelse((test_pred$glm.probs.test >0.6 & test_pred$glm.probs.test <= 0.7), "0.6 - 0.7", 
                         ifelse((test_pred$glm.probs.test >0.7 & test_pred$glm.probs.test <= 0.8), "0.7 - 0.8", 
                         ifelse((test_pred$glm.probs.test >0.8 & test_pred$glm.probs.test <= 0.9), "0.8 - 0.9", 
                         ifelse((test_pred$glm.probs.test >0.9 & test_pred$glm.probs.test <= 1), "0.9 - 1","" )
                         )))))))))

fetch_leads_geo_region<- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/geo_region.sql"))
  geo_region <- run_query(source = "oms", query_statement = query)
  
  return(geo_region)
}

fetch_hh_leads_geo_region <- function(test_pred) {
  leads_geo_region <- fetch_leads_geo_region(hh_leads)
  leads_with_geo_region <- test_pred %>% left_join(leads_geo_region)
  return(leads_with_geo_region)
}

test_pred_geo_region = fetch_hh_leads_geo_region(test_pred)

test_pred_geo_region = test_pred_geo_region %>% filter(test_pred_geo_region$prob != "0 - 0.1" & test_pred_geo_region$prob != "0.1 - 0.2" &
                                                         test_pred_geo_region$prob != "0.9 - 1")

names(test_pred_geo_region)
test_pred_geo_region_subset = select(test_pred_geo_region,"phone_number","customer_name","first_order_date","second_order_date","third_order_date",
                                     "geo_region","prob")


test_pred_geo_region_subset$LTO <- ifelse(is.na(test_pred_geo_region_subset$first_order_date) == FALSE &
                                            is.na(test_pred_geo_region_subset$second_order_date) == TRUE , 1,
                                          ifelse(is.na(test_pred_geo_region_subset$second_order_date) == FALSE &
                                                   is.na(test_pred_geo_region_subset$third_order_date) == TRUE , 2,0))


test_pred_geo_region_subset <- select(test_pred_geo_region_subset,"phone_number","customer_name","LTO","geo_region","prob")
test_pred_geo_region_subset$created_at = Sys.Date()

test_pred_geo_region_subset_0.2_0.3 = test_pred_geo_region_subset %>% filter(test_pred_geo_region_subset$prob == "0.2 - 0.3")
test_pred_geo_region_subset_0.3_0.4 = test_pred_geo_region_subset %>% filter(test_pred_geo_region_subset$prob == "0.3 - 0.4")
test_pred_geo_region_subset_0.4_0.5 =test_pred_geo_region_subset %>% filter(test_pred_geo_region_subset$prob == "0.4 - 0.5")
test_pred_geo_region_subset_0.5_0.6 =test_pred_geo_region_subset %>% filter(test_pred_geo_region_subset$prob == "0.5 - 0.6")
test_pred_geo_region_subset_0.6_0.7 =test_pred_geo_region_subset %>% filter(test_pred_geo_region_subset$prob == "0.6 - 0.7")
test_pred_geo_region_subset_0.7_0.8 =test_pred_geo_region_subset %>% filter(test_pred_geo_region_subset$prob == "0.7 - 0.8")
test_pred_geo_region_subset_0.8_0.9 =test_pred_geo_region_subset %>% filter(test_pred_geo_region_subset$prob == "0.8 - 0.9")

test_group_0.2_0.3 = sample_frac(test_pred_geo_region_subset_0.2_0.3,(1/1.7))
control_0.2_0.3 = sample_frac(test_pred_geo_region_subset_0.2_0.3, (0.7/1.7))

test_group_0.3_0.4 = sample_frac(test_pred_geo_region_subset_0.3_0.4,(1/1.7))
control_0.3_0.4 = sample_frac(test_pred_geo_region_subset_0.3_0.4, (0.7/1.7))

test_group_0.4_0.5 = sample_frac(test_pred_geo_region_subset_0.4_0.5,(1/1.7))
control_0.4_0.5 = sample_frac(test_pred_geo_region_subset_0.4_0.5, (0.7/1.7))

test_group_0.5_0.6 = sample_frac(test_pred_geo_region_subset_0.5_0.6,(1/1.7))
control_0.5_0.6 = sample_frac(test_pred_geo_region_subset_0.5_0.6, (0.7/1.7))

test_group_0.6_0.7 = sample_frac(test_pred_geo_region_subset_0.6_0.7,(1/1.7))
control_0.6_0.7 = sample_frac(test_pred_geo_region_subset_0.6_0.7, (0.7/1.7))

test_group_0.7_0.8 = sample_frac(test_pred_geo_region_subset_0.7_0.8,(1/1.7))
control_0.7_0.8 = sample_frac(test_pred_geo_region_subset_0.7_0.8, (0.7/1.7))

test_group_0.8_0.9 = sample_frac(test_pred_geo_region_subset_0.8_0.9,(1/1.7))
control_0.8_0.9 = sample_frac(test_pred_geo_region_subset_0.8_0.9, (0.7/1.7))


test_group_with_prob = rbind(test_group_0.2_0.3,test_group_0.3_0.4,test_group_0.4_0.5,test_group_0.5_0.6,test_group_0.6_0.7,test_group_0.7_0.8,test_group_0.8_0.9)
test_group = select(test_group_with_prob,"phone_number","customer_name","LTO","geo_region","created_at")

control = rbind(control_0.2_0.3,control_0.3_0.4,control_0.4_0.5,control_0.5_0.6,control_0.6_0.7,control_0.7_0.8,control_0.8_0.9)
#test_pred_geo_region_subset_backup = test_pred_geo_region_subset
# control = sample_frac(test_pred_geo_region_subset, (0.7/1.7))
# test_group = sample_frac(test_pred_geo_region_subset,(1/1.7))

test_pred_geo_region_subset$created_at = Sys.Date()
ss =gs4_create(name = "HH Leads Calling Sheet", sheets = c("Leads Calling Sheet","Historical leads"))

gs4_create(name = "HH Leads Calling Sheet control group", sheets = c("Control group"))

gs4_auth()
##sheet_add(ss="1GqHIN_vf2CuwghTOMPVLTe-9Nj30Hcu26g6o2qmlYF4", sheet ="Remaining leads scores")
##sheet_add(ss="1GqHIN_vf2CuwghTOMPVLTe-9Nj30Hcu26g6o2qmlYF4", sheet= "Remaining leads Historical scores")
sheet_add(ss="1ZkyoYCJfBaryb4GgHpCa7MMcz1W-1R3nekryZLMk7QA", sheet= "Test group")

sheet_write(test_group,ss="19ARhxiAtA4aD86aResHsRx83rdKBnHa9kiycdyOYh6A", sheet = "Leads Calling Sheet" )
sheet_append(test_group,ss="19ARhxiAtA4aD86aResHsRx83rdKBnHa9kiycdyOYh6A",sheet = "Historical leads" )

sheet_write(test_group_with_prob,ss="1ZkyoYCJfBaryb4GgHpCa7MMcz1W-1R3nekryZLMk7QA", sheet = "Test group" )
sheet_append(control,ss="1ZkyoYCJfBaryb4GgHpCa7MMcz1W-1R3nekryZLMk7QA", sheet = "Control group" )

test_pred_converted <- test_pred_geo_region %>% filter(whether_converted == 0)
test_pred_1 <- test_pred %>% group_by(prob,whether_converted) %>% summarise(total = n()) %>% arrange(prob,whether_converted)

test_pred_geo_region$first_order_date_1 = as.Date(test_pred_geo_region$first_order_date, tz ="")
test_pred_1 <- test_pred_geo_region %>% group_by(prob,first_order_date_1) %>% summarise(total = n()) %>% arrange(first_order_date_1)
test_pred_converted_1 <- test_pred_converted %>% group_by(prob,whether_converted) %>% summarise(total = n()) 

install.packages("reshape2")
library(reshape2)

test_pred_2 <- dcast(test_pred_1, prob ~ whether_converted, value.var="total", fun.aggregate=sum)
test_pred_2$conversion <- (test_pred_2$"1"/(test_pred_2$"0" + test_pred_2$"1"))*100

summary(check$whether_converted.y)

plot_1 = ggplot(test_pred_1, aes(x = prob, y = total))+
  geom_col(aes(fill = whether_converted), width = 0.5)
plot_1


plot_2 = ggplot(test_pred_1, aes(x=prob, y=total, fill=whether_converted)) + 
  geom_bar(stat="identity", position="dodge")
plot_2
 
plot_3 = ggplot(test_pred_2, aes(x = prob, y = conversion))+
  geom_col( width = 0.5)
plot_3

#1801
#4134

#install.packages("ROCR")
library(ROCR)
ROCRpred <- prediction(glm.probs.test, test$whether_converted)
ROCRperf <- performance(ROCRpred, 'tpr','fpr')
plot(ROCRperf, colorize = TRUE, text.adj = c(-0.2,1.7))

auc.tmp <- performance(ROCRpred,"auc");
auc <- as.numeric(auc.tmp@y.values)
auc

mean(glm.probs.test)
median(glm.probs.test)
max(glm.probs.test)
min(glm.probs.test)




mean(glm.pred.test == whether_converted)

perform_chisquare_test <- function(data, column1, column2) {
  # Subset df for column1.not_null AND column2.not_null
  required_data <- data %>%
    filter(!is.na(column1)) %>%
    filter(!is.na(column2))
  # Prepare contingency table
  frequency_info <- table(required_data[[column1]], required_data[[column2]])
  # prepare proportion table
  proportion_info <- prop.table(table(required_data[[column1]], required_data[[column2]]), margin = 2) * 100
  # Run statistical test
  statistical_test <- chisq.test(frequency_info)
  metric_list <- list(
    "factor" = column1,
    "count_of_factors" = frequency_info,
    "proportion_of_factors" = proportion_info,
    "statistical_test" = statistical_test
  )
  return(metric_list)
}

perform_hh_leads_conversion_eda <- function(first_order_vehicle_type) {
  list_of_metrics <- list("customer_frequency", "app_download_time_of_day", "whether_verified", "whether_bought_gold", "industry_type", "customer_type", "email_type", 
                          "number_of_session", "time_spent_bucket", "total_clicks_bucket", "whether_recharged","whether_installed_competitor","whether_installed_business","whether_installed_payment")
  chi_square_result <- c()
  for (metric in list_of_metrics) {
    chi_square_result <- perform_chisquare_test(data = first_order_vehicle_type, column1 = metric, column2 = metric)
    print(chi_square_result)
  }
  return(chi_square_result)
}

summary(factor(order_tracked_testing$industry_type))
perform_hh_leads_conversion_eda(order_tracked_testing)
 prop.table(table(first_order_vehicle_type[["whether_converted"]], first_order_vehicle_type[["wheth"]]), margin = 2) * 100

plot(order_tracked_testing$first_order_after_app_install,order_tracked_testing,whether_converted)

library(randomForest)

forest_output = randomForest(whether_converted ~ app_download_time_of_day  +first_order_after_app_install + customer_frequency + whether_bought_gold  + whether_recharged 
                             + whether_installed_competitor  + number_of_session  + email_type  + whether_installed_business + whether_installed_payment 
                               + vehicle_type + device_type + whether_cancelled +driver_rating_1+first_and_second_order_flag, data = train_new)
plot(forest_output)
print(forest_output)
print(importance(forest_output,type = 2)) 



pred = predict(forest_output, newdata=test[-7], type = "response")
pred_1 = pred[,2]
pred_2 <- ifelse(pred_1 > 0.5, 1, 0)
cm = table(test[,7], pred)
cm 

accuracy_Test <- sum(diag(cm)) / sum(cm)
accuracy_Test
precision = cm[2,2]/sum(cm[,2])
precision
recall= cm[2,2]/sum(cm[2,])
recall

F_measure = (2 * precision * recall) / (precision + recall)
F_measure

rsq(forest_output)

test_pred <- data.frame(order_tracked_testing_test_subset,pred_1)
class(test_pred$pred_1)
test_pred$prob <- ifelse((test_pred$pred_1 >=0 & test_pred$pred_1 <= 0.1), "0 - 0.1",
                         ifelse((test_pred$pred_1 >0.1 & test_pred$pred_1 <= 0.2), "0.1 - 0.2", 
                                ifelse((test_pred$pred_1 >0.2 & test_pred$pred_1 <= 0.3), "0.2 - 0.3", 
                                       ifelse((test_pred$pred_1 >0.3 & test_pred$pred_1 <= 0.4), "0.3 - 0.4", 
                                              ifelse((test_pred$pred_1 >0.4 & test_pred$pred_1 <= 0.5), "0.4 - 0.5", 
                                                     ifelse((test_pred$pred_1 >0.5 & test_pred$pred_1 <= 0.6), "0.5 - 0.6", 
                                                            ifelse((test_pred$pred_1 >0.6 & test_pred$pred_1 <= 0.7), "0.6 - 0.7", 
                                                                   ifelse((test_pred$pred_1 >0.7 & test_pred$pred_1 <= 0.8), "0.7 - 0.8", 
                                                                          ifelse((test_pred$pred_1 >0.8 & test_pred$pred_1 <= 0.9), "0.8 - 0.9", 
                                                                                 ifelse((test_pred$pred_1 >0.9 & test_pred$pred_1 <= 1), "0.9 - 1","" )
                                                                          )))))))))

test_pred_1 <- test_pred %>% group_by(prob,whether_converted) %>% summarise(total = n()) %>% arrange(prob,whether_converted)


install.packages("reshape2")
library(reshape2)

test_pred_2 <- dcast(test_pred_1, prob ~ whether_converted, value.var="total", fun.aggregate=sum)
test_pred_2$conversion <- (test_pred_2$"1"/(test_pred_2$"0" + test_pred_2$"1"))*100


order_tracked_testing$first_order_date_1 =as.Date(order_tracked_testing$first_order_date) + 15
order_tracked_testing[,c("first_order_date","first_order_date_1")]




