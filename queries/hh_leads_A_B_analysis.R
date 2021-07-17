library("googlesheets4")
library(reshape2)

gs4_auth()
control_group = read_sheet("https://docs.google.com/spreadsheets/d/1ZkyoYCJfBaryb4GgHpCa7MMcz1W-1R3nekryZLMk7QA/",sheet = "Control group")

control_group$phone_number = as.character(control_group$phone_number)

fetch_order_details <- function(control_group) {
  phone_number_list <- paste("'", paste(control_group$phone_number, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/A_B_analysis.sql"))
  get_order_details <- run_query(source = "sfms", query_statement = query)
  return(get_order_details)
}
leads_order_details = fetch_order_details(control_group)


class(control_group$phone_number)
control_group_details = control_group %>% left_join(leads_order_details,by = c("phone_number"="number"))
control_group_details = control_group_details %>%  mutate(whether_converted = ifelse(difftime( third_order_date, first_order_date,units = "days")<= 30 , 1,0 ))
control_group_details$whether_converted[is.na(control_group_details$whether_converted)] = 0


control_group_details_1 <- control_group_details %>% group_by(created_at,prob,whether_converted) %>% summarise(total = n()) %>% arrange(created_at,prob,whether_converted)
control_group_details_2 <- dcast(control_group_details_1, created_at + prob ~ whether_converted, value.var="total", fun.aggregate=sum)


test_group = read_sheet("https://docs.google.com/spreadsheets/d/1ZkyoYCJfBaryb4GgHpCa7MMcz1W-1R3nekryZLMk7QA/",sheet = "Test group")
test_group$phone_number = as.character(test_group$phone_number)

fetch_order_details <- function(control_group) {
  phone_number_list <- paste("'", paste(control_group$phone_number, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/queries/A_B_analysis.sql"))
  get_order_details <- run_query(source = "sfms", query_statement = query)
  return(get_order_details)
}
test_leads_order_details = fetch_order_details(test_group)
test_group_details = test_group %>% left_join(test_leads_order_details,by = c("phone_number"="number"))
test_group_details = test_group_details %>%  mutate(whether_converted = ifelse(difftime( third_order_date, first_order_date,units = "days")<= 30 , 1,0 ))
test_group_details$whether_converted[is.na(test_group_details$whether_converted)] = 0


test_group_details_1 <- test_group_details %>% group_by(created_at,prob,whether_converted) %>% summarise(total = n()) %>% arrange(created_at,prob,whether_converted)
test_group_details_2 <- dcast(test_group_details_1, created_at + prob ~ whether_converted, value.var="total", fun.aggregate=sum)


write.csv(control_group_10_may,"~/Documents/queries/control_10.csv")

control_group_10_may = read.csv("~/Documents/queries/control_10.csv")
  
control_group_10_may = sample_frac(control_group_10_may,0.43)

write.csv(control_group_details_2,"~/Documents/queries/control_group_details_1.csv")
write.csv(test_group_details_2,"~/Documents/queries/test_group_details_1.csv")

