# =============================================================================
# NC - Kindergarten Immunization and Exemption by County (2020-2023)
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

parse_pct_points <- function(x) {
  y <- readr::parse_number(as.character(x))
  if (all(is.na(y))) return(y)
  if (max(y, na.rm = TRUE) <= 1) return(y * 100)
  y
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_path <- "./raw/North Carolina Vaccine Exemption.xlsx"
  data_raw <- readxl::read_excel(raw_path, sheet = "Kindergarten by county 2020-23")

  data_clean <- data_raw %>%
    transmute(
      county = str_to_title(str_trim(County)),
      year = as.integer(Year),
      time = as.Date(paste0(Year, "-09-01")),
      enrollment = readr::parse_number(as.character(`Total Enrollment`)),
      n_medical_exempt = readr::parse_number(as.character(`Medical Exemption (Count)`)),
      pct_medical_exempt = parse_pct_points(`Medical Exemption (%)`),
      n_personal_exempt = readr::parse_number(as.character(`Religious Exemption (Count)`)),
      pct_personal_exempt = parse_pct_points(`Religious Exemption (%)`)
    ) %>%
    mutate(
      N_full_exempt = if_else(
        is.na(n_medical_exempt) & is.na(n_personal_exempt),
        NA_real_,
        coalesce(n_medical_exempt, 0) + coalesce(n_personal_exempt, 0)
      ),
      pct_full_exempt = if_else(
        is.na(pct_medical_exempt) & is.na(pct_personal_exempt),
        NA_real_,
        coalesce(pct_medical_exempt, 0) + coalesce(pct_personal_exempt, 0)
      )
    )

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "NC") %>%
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
      geography = if_else(is.na(geography), state_fips[1], geography),
      geography_name = county,
      grade = "Kindergarten",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt = n_personal_exempt,
      N_medical_exempt = n_medical_exempt,
      N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt,
      pct_medical_exempt,
      pct_full_exempt,
      enrollment
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
