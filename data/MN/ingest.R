# =============================================================================
# MN - Kindergarten Vaccination Coverage by County (2023-24)
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readxl)
library(readr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_end_year <- function(path) {
  m <- str_match(basename(path), "(20\\d{2})-(\\d{2})")
  if (is.na(m[1, 1])) return(NA_integer_)
  start_year <- m[1, 2]
  end_two <- m[1, 3]
  as.integer(paste0(substr(start_year, 1, 2), end_two))
}

parse_pct_points <- function(x) {
  y <- readr::parse_number(as.character(x))
  if (all(is.na(y))) return(y)
  if (max(y, na.rm = TRUE) <= 1) return(y * 100)
  y
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_path <- "./raw/kg_county_2023-24.xlsx"
  end_year <- parse_end_year(raw_path)
  time <- as.Date(paste0(end_year, "-09-01"))

  raw <- readxl::read_excel(raw_path, sheet = "K_County", col_names = FALSE)
  header <- raw[2, ] %>% unlist(use.names = FALSE)
  data_raw <- raw[-c(1, 2), ]
  names(data_raw) <- header

  data_clean <- data_raw %>%
    rename_with(~ str_replace_all(.x, "\\s+", " ")) %>%
    mutate(across(everything(), as.character)) %>%
    transmute(
      county = str_to_title(str_trim(`County`)),
      enrollment = readr::parse_number(`Kindergarten Enrollment`),
      pct_dtap = parse_pct_points(`DTaP % Vaccinated`),
      pct_polio = parse_pct_points(`Polio % Vaccinated`),
      pct_mmr = parse_pct_points(`MMR % Vaccinated`),
      pct_hep_b = parse_pct_points(`Hep B % Vaccinated`),
      pct_varicella = parse_pct_points(`Varicella % Vaccinated`),
      pct_dtap_nonmedical = parse_pct_points(`DTaP % non-medical`),
      pct_dtap_medical = parse_pct_points(`DTaP % medical`),
      pct_polio_nonmedical = parse_pct_points(`Polio % non-medical`),
      pct_polio_medical = parse_pct_points(`Polio % medical`),
      pct_mmr_nonmedical = parse_pct_points(`MMR % non-medical`),
      pct_mmr_medical = parse_pct_points(`MMR % medical`),
      pct_hep_b_nonmedical = parse_pct_points(`Hep B % non-medical`),
      pct_hep_b_medical = parse_pct_points(`Hep B % medical`),
      pct_varicella_nonmedical = parse_pct_points(`Varicella % non-medical`),
      pct_varicella_medical = parse_pct_points(`Varicella % medical`),
      pct_varicella_disease_history = parse_pct_points(`Varicella % Disease History`)
    ) %>%
    filter(!is.na(county)) %>%
    mutate(
      time = time,
      grade = "Kindergarten"
    )

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "MN") %>%
    mutate(geography_name = gsub(" County$", "", geography_name))

  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)

  data_out <- data_clean %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = c("county" = "geography_name")
    ) %>%
    mutate(
      geography = if_else(county == "Statewide", state_fips[1], geography),
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
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      enrollment,
      pct_dtap_nonmedical, pct_dtap_medical,
      pct_polio_nonmedical, pct_polio_medical,
      pct_mmr_nonmedical, pct_mmr_medical,
      pct_hep_b_nonmedical, pct_hep_b_medical,
      pct_varicella_nonmedical, pct_varicella_medical,
      pct_varicella_disease_history
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
