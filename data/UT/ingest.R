# =============================================================================
# UT - Vaccine Exemption Rates by School District (2018-2023)
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readxl)
library(readr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_end_year <- function(year_str) {
  m <- str_match(year_str, "(20\\d{2})-(20\\d{2})")
  if (!is.na(m[1, 1])) return(as.integer(m[1, 3]))
  m2 <- str_match(year_str, "(20\\d{2})-(\\d{2})")
  if (is.na(m2[1, 1])) return(NA_integer_)
  start_year <- m2[1, 2]
  end_two <- m2[1, 3]
  as.integer(paste0(substr(start_year, 1, 2), end_two))
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_path <- "./raw/Utah Vaccine Exemption.xlsx"
  sheets <- readxl::excel_sheets(raw_path)

  data_all <- bind_rows(lapply(sheets, function(sh) {
    grade <- case_when(
      str_detect(tolower(sh), "kindergarten") ~ "Kindergarten",
      str_detect(tolower(sh), "7th") ~ "7th grade",
      str_detect(tolower(sh), "k-12") ~ "K-12",
      TRUE ~ sh
    )

    d <- readxl::read_excel(raw_path, sheet = sh)

    d %>%
      transmute(
        health_district = str_trim(`Health District`),
        school_district = str_trim(`School Distric`),
        year = `Year`,
        end_year = parse_end_year(`Year`),
        time = as.Date(paste0(end_year, "-09-01")),
        pct_full_exempt = readr::parse_number(as.character(`Total Exemption Rate (%)`)),
        grade = grade
      )
  }))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips <- all_fips %>%
    filter(state == "UT", nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)

  data_out <- data_all %>%
    mutate(
      geography = state_fips[1],
      geography_name = "Utah",
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
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      health_district, school_district
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
