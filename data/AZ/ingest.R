library(dcf)
library(tidyverse)

## change here the 2 digit code being processed here
select.state = 'AZ'

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw", "csv", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()

# process raw if state has changed
if (!identical(process$raw_state, raw_state)) {
  
  #In this example, each year is saved as a separate file
  #The lapply reads in each of the years and saves in a list
  data.ls <- lapply(list.files('./raw', full.names = T), function(X) {
    #  print(X)
    read_csv(X) %>%
      mutate(Enrolled = as.numeric(Enrolled)) %>%
      reshape2::melt(., id.vars = c("School Year", "County", "Grade", "Enrolled")) %>%
      mutate(value = as.numeric(value))
  })
  
  #Reads in a dataframe that has all FIPS codes for the US
  fips_df <- vroom::vroom('../../resources/all_fips.csv.gz') %>%
    filter(state == 'AZ') %>%
    mutate(geography_name = gsub(' County', '', geography_name))
  
  #Combine all years together using bind_rows(), then format
  data <- data.ls %>%
    bind_rows() %>%
    rename(
      year = 'School Year',
      county = County,
      grade = Grade,
      N = Enrolled
    ) %>%
    left_join(fips_df, by = c('county' = 'geography_name')) %>%
    mutate(
      variable = gsub('% ', '', variable),
      doses = substr(variable, 1, 1),
      vax = tolower(variable),
      vax = gsub("[^A-Za-z]", "_", vax),
      vax = gsub('__', '', vax),
      county = if_else(county %in% c('Total', 'State Totals'), 'Total', county),
      geography = if_else(county == 'Total', '04', geography),
      vax = if_else(vax == 'exempt_from_every_req_d_vaccine', 'full_exempt', vax),
      vax = gsub('_mmr', 'mmr', vax),
      time = paste0(year, '-09-01')
    ) %>%
    rename(geography_name = county) %>%
    rename(!!paste0("N_", select.state) := N,!!paste0("pct_", select.state) := value)  %>%
    dplyr::select(time,
                  geography,
                  geography_name,
                  vax,
                  starts_with('N_'),
                  starts_with('pct_'))
  
  #Save standard file as a compressed csv
  vroom::vroom_write(data, './standard/data.csv.gz')
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
