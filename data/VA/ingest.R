library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)

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
      grade = parse_grade(Grade),
      n_medical = as.numeric(`Medical Exemptions`),
      n_personal = as.numeric(`Religious Exemptions`),
      enrollment = as.numeric(`Total Enrolled`)
    ) %>%
    mutate(
      start_year = str_extract(school_year, "\\d{4}$"),
      time = as.Date(paste0(start_year, "-09-01"))
    ) %>%
    filter(!is.na(time), !is.na(county), county != "")

  va_agg <- va_clean %>%
    group_by(time, county, grade) %>%
    summarize(
      enrollment = sum(enrollment, na.rm = TRUE),
      n_medical = sum(n_medical, na.rm = TRUE),
      n_personal = sum(n_personal, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      n_full = n_medical + n_personal,
      pct_medical = if_else(enrollment > 0, (n_medical / enrollment) * 100, NA_real_),
      pct_personal = if_else(enrollment > 0, (n_personal / enrollment) * 100, NA_real_),
      pct_full = if_else(enrollment > 0, (n_full / enrollment) * 100, NA_real_)
    )

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

  data_out <- va_agg %>%
    left_join(va_fips, by = "county") %>%
    mutate(
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_
    ) %>%
    filter(!is.na(geography)) %>%
    transmute(
      time,
      geography,
      geography_name = county,
      grade,
      N_dtap,
      N_polio,
      N_mmr,
      N_hep_b,
      N_varicella,
      N_personal_exempt = n_personal,
      N_medical_exempt = n_medical,
      N_full_exempt = n_full,
      pct_dtap,
      pct_polio,
      pct_mmr,
      pct_hep_b,
      pct_varicella,
      pct_personal_exempt = pct_personal,
      pct_medical_exempt = pct_medical,
      pct_full_exempt = pct_full,
      enrollment
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
