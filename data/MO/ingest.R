library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  raw_path <- "./raw/Missouri Vaccine Exemption.xlsx"
  sheet_map <- list(
    "Kindergarten" = "Kindergarten",
    "7th Grade" = "7th grade",
    "K-12" = "K-12"
  )
  
  parse_sheet <- function(sh, grade_label) {
    data_raw <- readxl::read_excel(raw_path, sheet = sh, skip = 3, col_names = FALSE)
    base_names <- c(
      "school_year", "county",
      "dtap_med", "dtap_rel", "dtap_total",
      "hep_b_med", "hep_b_rel", "hep_b_total",
      "polio_med", "polio_rel", "polio_total",
      "mmr_med", "mmr_rel", "mmr_total",
      "varicella_med", "varicella_rel", "varicella_total"
    )
    if (ncol(data_raw) > length(base_names)) {
      extra <- paste0("skip", seq_len(ncol(data_raw) - length(base_names)))
      names(data_raw) <- c(base_names, extra)
    } else {
      names(data_raw) <- base_names[seq_len(ncol(data_raw))]
    }
    data_raw <- data_raw %>% mutate(across(everything(), as.character))
    
    # The workbook is titled "Exemption Rates for Kindergarteners in Missouri
    # Schools, by Vaccine Series", and each series has Medical / Religious /
    # Total columns. These are exemption shares, not coverage, so they are named
    # as exemptions here and the coverage columns are left missing: DHSS
    # publishes no coverage, and 1 - exemption is not coverage (a student can be
    # unvaccinated without an exemption, and an exempt student may still have
    # had the vaccine).
    #
    # The medical and religious components are now kept as well; previously only
    # the total was read and the split was discarded.
    data_raw %>%
      transmute(
        school_year = school_year,
        county = county,
        grade = grade_label,
        pct_dtap_exempt = dtap_total,
        pct_dtap_medical_exempt = dtap_med,
        pct_dtap_religious_exempt = dtap_rel,
        pct_hep_b_exempt = hep_b_total,
        pct_hep_b_medical_exempt = hep_b_med,
        pct_hep_b_religious_exempt = hep_b_rel,
        pct_polio_exempt = polio_total,
        pct_polio_medical_exempt = polio_med,
        pct_polio_religious_exempt = polio_rel,
        pct_mmr_exempt = mmr_total,
        pct_mmr_medical_exempt = mmr_med,
        pct_mmr_religious_exempt = mmr_rel,
        pct_varicella_exempt = varicella_total,
        pct_varicella_medical_exempt = varicella_med,
        pct_varicella_religious_exempt = varicella_rel
      )
  }
  
  data_all <- bind_rows(lapply(names(sheet_map), function(sh) {
    parse_sheet(sh, sheet_map[[sh]])
  })) %>%
    mutate(
      year_end = str_extract(school_year, "\\d{4}$"),
      time = as.Date(school_year_time_from_end(year_end)),
      across(starts_with("pct_"), ~ parse_rate(.x, from = "rate"))
    ) %>%
    filter(!is.na(time))
  
  # "St. Louis City" and "St. Louis County" are separate FIPS (29510 and
  # 29189); stripping " County" off the reference names used to make both
  # unmatchable, so 36 rows landed on the state total instead.
  data_out <- data_all %>%
    join_county_fips("MO") %>%
    mutate(
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      # Coverage: not published by this source (see parse_sheet above).
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      # DHSS reports exemptions per vaccine series, not one figure across all
      # series, so there is no all-series medical/religious/any total to give.
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_,
      pct_full_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      pct_dtap_exempt, pct_dtap_medical_exempt, pct_dtap_religious_exempt,
      pct_polio_exempt, pct_polio_medical_exempt, pct_polio_religious_exempt,
      pct_mmr_exempt, pct_mmr_medical_exempt, pct_mmr_religious_exempt,
      pct_hep_b_exempt, pct_hep_b_medical_exempt, pct_hep_b_religious_exempt,
      pct_varicella_exempt, pct_varicella_medical_exempt,
      pct_varicella_religious_exempt
    )
  
  write_standard(data_out, "Missouri", "./standard/data.csv.gz", from = "rate")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
