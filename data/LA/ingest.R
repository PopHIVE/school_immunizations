library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/county_fips.R")

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
  
  data_out <- data_raw %>%
    rename(
      grade = Grade,
      school_year = SchoolYear,
      parish = Parish,
      pct_full_exempt = Exemptions,
      pct_complete = `Complete Records`,
      pct_dtap = `DTaP/TD (>=4)`,
      pct_tdap = `Tdap (>=1)`,
      pct_polio = `Polio (>=3)`,
      pct_mmr = `MMR (>=2)`,
      pct_hep_b = `HepB (>=3)`,
      pct_hep_a = `HepA (>=2)`,
      pct_varicella = `Var (>=2)`
    ) %>%
    mutate(
      year_start = str_extract(school_year, "^\\d{4}"),
      time = as.Date(paste0(year_start, "-09-01")),
      pct_full_exempt = as.numeric(pct_full_exempt),
      pct_complete = as.numeric(pct_complete),
      pct_dtap = as.numeric(pct_dtap),
      pct_tdap = as.numeric(pct_tdap),
      pct_polio = as.numeric(pct_polio),
      pct_mmr = as.numeric(pct_mmr),
      pct_hep_b = as.numeric(pct_hep_b),
      pct_hep_a = as.numeric(pct_hep_a),
      pct_varicella = as.numeric(pct_varicella),
      # MenACWY is published as (>=1) for 6th grade and (>=2) for 11th grade,
      # in mutually exclusive rows, so this is a merge, not an overwrite.
      pct_mcv4 = coalesce(as.numeric(`MenACWY (>=1)`), as.numeric(`MenACWY (>=2)`))
    ) %>%
    filter(!is.na(time)) %>%
    join_county_fips("LA", county_col = "parish") %>%
    mutate(
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_tdap, pct_polio, pct_mmr, pct_hep_b, pct_hep_a,
      pct_varicella, pct_mcv4,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt, pct_complete
    )
  
  write_standard(data_out, "Louisiana", "./standard/data.csv.gz", from = "rate")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
