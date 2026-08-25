library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)

  # Every remaining file is a single school year, labelled by its start and end
  # ("Medical Exemptions 2023-2024.xlsx" -> 2023-24), dated to the September it
  # started. The multi-year "Medical Exemptions 2019-2022.xlsx" cumulative
  # workbook (three school years with no per-row date to split them) has been
  # dropped from raw/ rather than kept and excluded downstream.
  parse_file <- function(path) {
    is_medical <- str_detect(basename(path), "Medical")
    year_match <- str_match(basename(path), "(\\d{4})\\s*-\\s*(\\d{4})")
    if (is.na(year_match[1, 1])) {
      stop("No YYYY-YYYY range in filename: ", basename(path))
    }
    year_end <- as.integer(year_match[1, 3])
    time <- as.Date(school_year_time_from_end(year_end))

    data_raw <- readxl::read_excel(path)
    data_raw %>%
      mutate(
        geography = sprintf("%05d", as.integer(COUNTY_CODE)),
        time = time
      ) %>%
      filter(!is.na(geography)) %>%
      count(geography, time, name = "exempt_count") %>%
      mutate(type = if_else(is_medical, "medical", "religious"))
  }

  data_all <- bind_rows(lapply(raw_files, parse_file)) %>%
    tidyr::pivot_wider(names_from = type, values_from = exempt_count)
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "MS")
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  data_out <- data_all %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = "geography"
    ) %>%
    mutate(
      # The standard label is the bare county name, as join_county_fips() emits
      # for the states that use it -- not all_fips' "DeSoto County".
      geography_name = sub("\\s+County$", "", geography_name),
      grade = "Overall",
      # MSDH publishes one record per exemption, with no enrolment anywhere in
      # the files, so these are counts with no denominator and there are no
      # rates to report.
      N_religious_exempt = religious,
      N_medical_exempt = medical
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_religious_exempt, N_medical_exempt
    )
  
  write_standard(data_out, "Mississippi", "./standard/data.csv.gz", from = "percent")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
