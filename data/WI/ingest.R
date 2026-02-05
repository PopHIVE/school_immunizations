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
  
  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)
  
  data_all <- bind_rows(lapply(raw_files, function(path) {
    year_match <- str_extract(basename(path), "\\d{4}")
    time <- as.Date(paste0(year_match, "-09-01"))
    
    data_raw <- readxl::read_excel(path, sheet = "By School")
    data_raw %>%
      transmute(
        county = County,
        enrollment = NA_real_,
        pct_medical_exempt = if ("% Health Waiver" %in% names(data_raw)) .data[["% Health Waiver"]] else NA_real_,
        pct_rel = if ("% Religious Waiver" %in% names(data_raw)) .data[["% Religious Waiver"]] else NA_real_,
        pct_personal = if ("% Personal Conviction Wavier" %in% names(data_raw)) .data[["% Personal Conviction Wavier"]] else NA_real_,
        time = time
      ) %>%
      mutate(
        pct_medical_exempt = readr::parse_number(as.character(pct_medical_exempt)),
        pct_rel = readr::parse_number(as.character(pct_rel)),
        pct_personal = readr::parse_number(as.character(pct_personal))
      ) %>%
      group_by(county, time) %>%
      summarize(
        pct_medical_exempt = mean(pct_medical_exempt, na.rm = TRUE),
        pct_personal_exempt = mean(pct_rel + pct_personal, na.rm = TRUE),
        .groups = "drop"
      )
  }))
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "WI") %>%
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
      grade = "Overall",
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
      pct_full_exempt = NA_real_
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
