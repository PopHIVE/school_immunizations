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
  
  raw_path <- "./raw/2014-2025 immunization exemption by school district.xlsx"
  data_raw <- readxl::read_excel(raw_path, skip = 2)
  
  year_cols <- names(data_raw)[str_detect(names(data_raw), "^\\d{4}-\\d{2} Students Exempted$")]
  data_long <- data_raw %>%
    select(all_of(year_cols)) %>%
    mutate(across(everything(), as.character)) %>%
    pivot_longer(cols = everything(), names_to = "year_label", values_to = "exempt_count") %>%
    mutate(
      exempt_count = readr::parse_number(as.character(exempt_count)),
      year_range = str_extract(year_label, "^\\d{4}-\\d{2}"),
      year_start = str_extract(year_range, "^\\d{4}"),
      year_end2 = str_extract(year_range, "\\d{2}$"),
      year_end = paste0(substr(year_start, 1, 2), year_end2),
      time = as.Date(paste0(year_end, "-09-01"))
    ) %>%
    filter(!is.na(time)) %>%
    group_by(time) %>%
    summarize(
      N_full_exempt = sum(exempt_count, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      geography = "05",
      geography_name = "Arkansas",
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_personal_exempt = NA_real_,
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
  
  vroom::vroom_write(data_long, "./standard/data.csv.gz")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
