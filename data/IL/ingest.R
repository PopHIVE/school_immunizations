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
  
  parse_one <- function(path) {
    year_pair <- str_match(basename(path), "_(\\d{4})_(\\d{2})")
    year_single <- str_match(basename(path), "_(\\d{4})")
    year_end <- if (!is.na(year_pair[2]) && !is.na(year_pair[3])) {
      paste0(substr(year_pair[2], 1, 2), year_pair[3])
    } else if (!is.na(year_single[2])) {
      year_single[2]
    } else {
      NA_character_
    }
    time <- if (!is.na(year_end)) as.Date(paste0(year_end, "-09-01")) else as.Date(NA)
    
    data_raw <- readxl::read_excel(path)
    data_raw %>%
      transmute(
        county = County,
        enrollment = `Enrollment PreK-12`,
        N_personal_exempt = `Religious objection`,
        N_medical_exempt = `Medical reasons`,
        time = time
      ) %>%
      filter(!is.na(time))
  }
  
  data_all <- bind_rows(lapply(raw_files, parse_one)) %>%
    mutate(
      enrollment = readr::parse_number(as.character(enrollment)),
      N_personal_exempt = readr::parse_number(as.character(N_personal_exempt)),
      N_medical_exempt = readr::parse_number(as.character(N_medical_exempt)),
      pct_personal_exempt = if_else(enrollment > 0, N_personal_exempt / enrollment, NA_real_),
      pct_medical_exempt = if_else(enrollment > 0, N_medical_exempt / enrollment, NA_real_)
    )
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "IL") %>%
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
