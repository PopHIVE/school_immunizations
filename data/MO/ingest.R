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
    
    data_raw %>%
      transmute(
        school_year = school_year,
        county = county,
        pct_dtap = dtap_total,
        pct_hep_b = hep_b_total,
        pct_polio = polio_total,
        pct_mmr = mmr_total,
        pct_varicella = varicella_total,
        grade = grade_label
      )
  }
  
  data_all <- bind_rows(lapply(names(sheet_map), function(sh) {
    parse_sheet(sh, sheet_map[[sh]])
  })) %>%
    mutate(
      year_end = str_extract(school_year, "\\d{4}$"),
      time = as.Date(paste0(year_end, "-09-01")),
      across(starts_with("pct_"), ~ readr::parse_number(as.character(.x)))
    ) %>%
    filter(!is.na(time))
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "MO") %>%
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
