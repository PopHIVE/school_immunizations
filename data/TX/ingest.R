#
# Download
#

# add files to the `raw` directory

#
# Reformat
#

# read from the `raw` directory, and write to the `standard` directory

# --- activate renv no matter where this script is run from ---

# Find project root by walking up until we see renv.lock

library(dcf)
library(tidyverse)
library(readxl)
library(dplyr)
library(stringr)
library(vroom)
library(readr)

## change here the 2 digit code being processed here
select.state = 'TX'

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw", "csv", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

# process raw if state has changed
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  infer_grade <- function(path) {
    fn <- tolower(basename(path))
    if (str_detect(fn, "kg")) return("Kindergarten")
    if (str_detect(fn, "7th")) return("7th grade")
    if (str_detect(fn, "k-12")) return("K-12")
    NA_character_
  }
  
  # In this example, each grade is saved as a separate file
  data.ls <- lapply(list.files("./raw", full.names = TRUE), function(x) {
    grade_label <- infer_grade(x)
    if (is.na(grade_label)) stop("Could not infer grade from filename: ", basename(x))
    
    read_csv(
      x,
      skip = 1,
      na = c("", "NA", "NR**"),
      show_col_types = FALSE
    ) %>%
      mutate(grade = grade_label) %>%
      reshape2::melt(., id.vars = c("State", "County", "grade")) %>%
      mutate(value = readr::parse_number(as.character(value)))
  })
  
  # Reads in a dataframe that has all FIPS codes for the US
  fips_df <- vroom::vroom("../../resources/all_fips.csv.gz") %>%
    filter(state == "TX") %>%
    mutate(geography_name = gsub(" County", "", geography_name))
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  fips_county <- fips_df %>%
    filter(nchar(geography) == 5)
  
  # Combine all years together using bind_rows(), then format
  data <- data.ls %>%
    bind_rows() %>%
    rename(
      year = variable,
      county = County
    ) %>%
    filter(str_detect(year, "^\\d{4}-\\d{4}$")) %>%
    left_join(fips_county, by = c("county" = "geography_name")) %>%
    mutate(
      county = if_else(county %in% c("Total", "State Totals"), "Total", county),
      geography = if_else(county == "Total", state_fips[1], geography),
      yearpart = sub(".*-", "", year),
      time = as.Date(paste0(yearpart, "-09-01")),
      geography_name = county,
      
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_personal_exempt = value,
      pct_medical_exempt = NA_real_,
      pct_full_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt
    )
  
  #Save standard file as a compressed csv
  vroom::vroom_write(data, './standard/data.csv.gz')
  
  # record processed raw state
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
