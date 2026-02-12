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
  
  parse_file <- function(path) {
    is_medical <- str_detect(basename(path), "Medical")
    year_match <- str_match(basename(path), "(\\d{4})-(\\d{4})")
    year_end <- if (!is.na(year_match[3])) year_match[3] else NA_character_
    time <- if (!is.na(year_end)) as.Date(paste0(year_end, "-12-31")) else as.Date(NA)
    
    data_raw <- readxl::read_excel(path)
    data_raw %>%
      mutate(
        geography = sprintf("%05d", as.integer(COUNTY_CODE)),
        time = time
      ) %>%
      filter(!is.na(geography)) %>%
      count(geography, time, name = "exempt_count") %>%
      mutate(type = if_else(is_medical, "medical", "personal"))
  }
  
  data_all <- bind_rows(lapply(raw_files, parse_file)) %>%
    tidyr::pivot_wider(names_from = type, values_from = exempt_count)
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "MS")
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  data_out <- data_all %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = "geography"
    ) %>%
    mutate(
      geography_name = geography_name,
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = personal,
      N_medical_exempt = medical,
      N_full_exempt = NA_real_,
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
  
  vroom::vroom_write(data_out, "./standard/data.csv.gz")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
