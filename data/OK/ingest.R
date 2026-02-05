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
    year_match <- str_match(basename(path), "(\\d{2})-(\\d{2})")
    year_end <- if (!is.na(year_match[2]) && !is.na(year_match[3])) {
      paste0("20", year_match[3])
    } else {
      NA_character_
    }
    time <- if (!is.na(year_end)) as.Date(paste0(year_end, "-09-01")) else as.Date(NA)
    
    data_raw <- readxl::read_excel(path, skip = 6, col_names = FALSE)
    names(data_raw) <- c(
      "county", "pct_dtap", "pct_polio", "pct_mmr", "pct_hep_b",
      "pct_hep_a", "pct_varicella", "pct_all",
      "pct_medical_exempt", "pct_personal_exempt", "pct_full_exempt"
    )[seq_len(ncol(data_raw))]
    
    data_raw %>%
      filter(!is.na(county), county != "County") %>%
      transmute(
        county = county,
        pct_dtap = readr::parse_number(as.character(pct_dtap)),
        pct_polio = readr::parse_number(as.character(pct_polio)),
        pct_mmr = readr::parse_number(as.character(pct_mmr)),
        pct_hep_b = readr::parse_number(as.character(pct_hep_b)),
        pct_varicella = readr::parse_number(as.character(pct_varicella)),
        pct_medical_exempt = readr::parse_number(as.character(pct_medical_exempt)),
        pct_personal_exempt = readr::parse_number(as.character(pct_personal_exempt)),
        time = time
      ) %>%
      filter(!is.na(pct_dtap))
  }))
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "OK") %>%
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
      grade = "Kindergarten",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
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
