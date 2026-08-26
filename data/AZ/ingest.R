library(dcf)
library(tidyverse)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")

## change here the 2 digit code being processed here
select.state = 'AZ'

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw", "csv", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

# process raw if state has changed
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  #In this example, each year is saved as a separate file
  #The lapply reads in each of the years and saves in a list
  data.ls <- lapply(list.files('./raw', full.names = T), function(X) {
    #  print(X)
    read_csv(X, show_col_types = FALSE) %>%
      mutate(Enrolled = readr::parse_number(as.character(Enrolled))) %>%
      reshape2::melt(., id.vars = c("School Year", "County", "Grade", "Enrolled")) %>%
      mutate(value = readr::parse_number(as.character(value)))
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
      # The denominator of every published percentage in the ADHS table, so it
      # is carried through as N_enrolled rather than dropped.
      N_enrolled = Enrolled
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
      yearpart = sub(".*-", "", year),
      time = school_year_time_from_end(yearpart),
      grade = 'Kindergarten'
    ) %>%
    rename(geography_name = county) %>%
    rename(pct = value) %>%
    dplyr::select(time,
                  geography,
                  geography_name,
                  grade,
                  N_enrolled,
                  vax,
                  starts_with('pct')) %>%
    pivot_wider(id_cols = c(time, geography, geography_name, grade, N_enrolled),
                names_from = vax,
                values_from = pct) %>%
    rename(
      pct_dtap = dtap,
      pct_polio = polio,
      pct_mmr = mmr,
      pct_hep_b = hep_b,
      pct_varicella = varicella,
      pct_personal_exempt = personal_exempt,
      pct_medical_exempt = medical_exempt,
      pct_full_exempt = full_exempt
    ) %>%
    mutate(time = as.Date(time)) %>%
    # No N_<measure> columns: ADHS publishes only the enrolment count and the
    # shares, not the vaccinated/exempt numerators.
    transmute(
      time, geography, geography_name, grade,
      N_enrolled,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt
    )
  
  #Save standard file as a compressed csv
  write_standard(data, "Arizona", './standard/data.csv.gz', from = "percent")
  
  # record processed raw state
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
