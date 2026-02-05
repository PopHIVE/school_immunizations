library(dcf)
library(dplyr)
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
  
  raw_path <- "./raw/Massachusetts Vaccine Exemption.xlsx"
  sheet_map <- list(
    "Kindergarten 20-25 by County" = "Kindergarten",
    "Grade 7 20-25 by County  " = "7th grade"
  )
  
  data_all <- bind_rows(lapply(names(sheet_map), function(sh) {
    grade_label <- sheet_map[[sh]]
    data_raw <- readxl::read_excel(raw_path, sheet = sh)
    data_raw %>%
      rename(
        county = County,
        school_year = Year,
        pct_medical_exempt = `Medical Exemption`,
        pct_personal_exempt = `Religious Exemption`,
        pct_full_exempt = `Total Exemption`
      ) %>%
      mutate(
        grade = grade_label,
        school_year = as.character(school_year),
        year_end = str_extract(school_year, "\\d{4}$"),
        time = as.Date(if_else(!is.na(year_end), paste0(year_end, "-09-01"), NA_character_)),
        pct_medical_exempt = as.numeric(pct_medical_exempt),
        pct_personal_exempt = as.numeric(pct_personal_exempt),
        pct_full_exempt = as.numeric(pct_full_exempt)
      ) %>%
      filter(!is.na(time))
  }))
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "MA") %>%
    mutate(geography_name = gsub(" County", "", geography_name))
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  data_out <- data_all %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = c("county" = "geography_name")
    ) %>%
    mutate(
      geography = if_else(is.na(geography), state_fips[1], geography),
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
      pct_varicella = NA_real_
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
