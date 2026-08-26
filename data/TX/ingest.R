source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
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
  
  # Combine all years together using bind_rows(), then format
  data <- data.ls %>%
    bind_rows() %>%
    rename(
      year = variable,
      county = County
    ) %>%
    filter(str_detect(year, "^\\d{4}-\\d{4}$")) %>%
    join_county_fips("TX", statewide = c("Total", "State Totals")) %>%
    mutate(
      yearpart = sub(".*-", "", year),
      time = as.Date(school_year_time_from_end(yearpart)),
      pct_conscientious_exemption = value
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      pct_conscientious_exemption
    )
  
  #Save standard file as a compressed csv
  write_standard(data, "Texas", './standard/data.csv.gz', from = "percent")
  
  # record processed raw state
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
