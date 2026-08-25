library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  va_raw <- readxl::read_excel("raw/VA_2019-2024.xlsx", sheet = "Exemption Data")

  parse_grade <- function(x) {
    x <- str_to_lower(str_squish(as.character(x)))
    case_when(
      str_detect(x, "kindergarten") ~ "Kindergarten",
      str_detect(x, "7th") ~ "7th grade",
      str_detect(x, "12th") ~ "12th grade",
      TRUE ~ str_to_title(x)
    )
  }

  county_key <- function(x) {
    x %>%
      as.character() %>%
      str_squish() %>%
      str_to_title()
  }

  va_clean <- va_raw %>%
    transmute(
      school_year = as.character(`School Year`),
      county = county_key(County),
      school_name = `School Name`,
      district = `School Division`,
      school_type = `School Type`,
      grade = parse_grade(Grade),
      N_medical_exempt = as.numeric(`Medical Exemptions`),
      N_personal_exempt = as.numeric(`Religious Exemptions`),
      N_enrolled = as.numeric(`Total Enrolled`)
    ) %>%
    mutate(
      start_year = str_extract(school_year, "\\d{4}$"),
      time = as.Date(paste0(start_year, "-09-01"))
    ) %>%
    filter(!is.na(time), !is.na(county), county != "")

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  va_fips <- all_fips %>%
    filter(state == "VA", nchar(geography) == 5) %>%
    transmute(
      geography,
      geography_name,
      county = geography_name %>%
        str_replace(" County$", "") %>%
        str_replace(" city$", " City") %>%
        str_squish()
    )

  va_joined <- va_clean %>%
    left_join(va_fips, by = "county")

  unmatched <- sort(unique(va_joined$county[is.na(va_joined$geography)]))
  if (length(unmatched)) {
    warning(length(unmatched), " county name(s) matched no VA FIPS and were dropped: ",
            paste(unmatched, collapse = ", "), call. = FALSE)
  }
  va_joined <- va_joined %>% filter(!is.na(geography))

  schools <- va_joined %>%
    mutate(
      type = "school",
      pct_medical_exempt = 100 * rate_from_counts(N_medical_exempt, N_enrolled),
      pct_personal_exempt = 100 * rate_from_counts(N_personal_exempt, N_enrolled)
    ) %>%
    transmute(
      time, geography, geography_name = county, type,
      school_name, district, school_type, grade,
      N_medical_exempt, N_personal_exempt, N_enrolled,
      pct_medical_exempt, pct_personal_exempt
    )

  counties <- va_joined %>%
    group_by(time, geography, county, grade) %>%
    summarize(
      N_medical_exempt = sum(N_medical_exempt, na.rm = TRUE),
      N_personal_exempt = sum(N_personal_exempt, na.rm = TRUE),
      N_enrolled = sum(N_enrolled, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      type = "county",
      school_name = NA_character_,
      district = NA_character_,
      school_type = NA_character_,
      pct_medical_exempt = 100 * rate_from_counts(N_medical_exempt, N_enrolled),
      pct_personal_exempt = 100 * rate_from_counts(N_personal_exempt, N_enrolled)
    ) %>%
    transmute(
      time, geography, geography_name = county, type,
      school_name, district, school_type, grade,
      N_medical_exempt, N_personal_exempt, N_enrolled,
      pct_medical_exempt, pct_personal_exempt
    )

  data_out <- bind_rows(schools, counties)

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "Virginia", "standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
