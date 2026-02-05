library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  raw_path <- "./raw/LA_parish_21-24.xlsx"
  data_raw <- readxl::read_excel(raw_path, sheet = "Sheet 1") %>%
    tidyr::fill(Grade, SchoolYear, .direction = "down")
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "LA") %>%
    mutate(geography_name = gsub(" Parish", "", geography_name))
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  data_out <- data_raw %>%
    rename(
      grade = Grade,
      school_year = SchoolYear,
      parish = Parish,
      pct_full_exempt = Exemptions
    ) %>%
    mutate(
      year_end = str_extract(school_year, "\\d{4}$"),
      time = as.Date(paste0(year_end, "-09-01")),
      pct_full_exempt = as.numeric(pct_full_exempt)
    ) %>%
    filter(!is.na(time)) %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = c("parish" = "geography_name")
    ) %>%
    mutate(
      geography = if_else(is.na(geography), state_fips[1], geography),
      geography_name = parish,
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
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt
    )
  
  vroom::vroom_write(data_out, "./standard/data.csv.gz")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
