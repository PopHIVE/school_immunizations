# =============================================================================
# VT - K-12 State and County Immunization & Exemption Percentages (2017-2025)
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readxl)
library(readr)
library(tidyr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_end_year <- function(school_year) {
  m <- str_match(school_year, "(20\\d{2})-(20\\d{2})")
  if (!is.na(m[1, 1])) return(as.integer(m[1, 3]))
  m2 <- str_match(school_year, "(20\\d{2})-(\\d{2})")
  if (is.na(m2[1, 1])) return(NA_integer_)
  start_year <- m2[1, 2]
  end_two <- m2[1, 3]
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

  raw_path <- "./raw/VT K-12 state and county-level data 2017 thru 2025.xlsx"
  raw <- readxl::read_excel(raw_path, sheet = "Sheet1", col_names = FALSE)

  header_idx <- which(raw[[1]] == "School Year")
  if (length(header_idx) > 0) {
    header <- as.character(raw[header_idx[1], ])
    data_raw <- raw[(header_idx[1] + 1):nrow(raw), ]
    names(data_raw) <- header

    data_long <- data_raw %>%
      filter(!is.na(`School Year`)) %>%
      transmute(
        school_year = as.character(`School Year`),
        unit = str_trim(`Unit of analysis`),
        public_independent = str_trim(`Public or Independent`),
        grade = str_trim(`Grades`),
        geography = str_trim(`Geography`),
        enrollment = readr::parse_number(as.character(`Enrollment`)),
        type = str_trim(`Immunization or Exemption`),
        exemption = str_trim(`Exemption`),
        immunization = str_trim(`Immunization`),
        value = parse_pct_points(coalesce(`Percent`, `Percentage`))
      )

    data_wide <- data_long %>%
      mutate(
        measure = case_when(
          type == "Exemption" & str_detect(exemption, "Medical") ~ "pct_medical_exempt",
          type == "Exemption" & str_detect(exemption, "Religious") ~ "pct_personal_exempt",
          type == "Exemption" & str_detect(exemption, "Provision") ~ "pct_provisional",
          type == "Immunization" & str_detect(immunization, "DTaP") ~ "pct_dtap",
          type == "Immunization" & str_detect(immunization, "Polio") ~ "pct_polio",
          type == "Immunization" & str_detect(immunization, "MMR") ~ "pct_mmr",
          type == "Immunization" & str_detect(immunization, "HepB") ~ "pct_hep_b",
          type == "Immunization" & str_detect(immunization, "Varicella") ~ "pct_varicella",
          type == "Immunization" & str_detect(immunization, "Full") ~ "pct_fully_immunized",
          TRUE ~ NA_character_
        )
      ) %>%
      filter(!is.na(measure)) %>%
      group_by(school_year, unit, public_independent, grade, geography, enrollment, measure) %>%
      summarize(
        value = if (all(is.na(value))) NA_real_ else max(value, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      pivot_wider(names_from = measure, values_from = value)

    all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
    fips_df <- all_fips %>%
      filter(state == "VT") %>%
      mutate(geography_name = gsub(" County$", "", geography_name))

    state_fips <- fips_df %>%
      filter(nchar(geography) == 2) %>%
      distinct(geography) %>%
      pull(geography)

    data_out <- data_wide %>%
      mutate(
        end_year = parse_end_year(school_year),
        time = as.Date(paste0(end_year, "-09-01")),
        geography_name = geography
      ) %>%
      select(-geography) %>%
      left_join(
        fips_df %>% filter(nchar(geography) == 5),
        by = c("geography_name" = "geography_name")
      ) %>%
      mutate(
        geography = if_else(unit == "County" & !is.na(geography), geography, state_fips[1]),
        N_dtap = NA_real_,
        N_polio = NA_real_,
        N_mmr = NA_real_,
        N_hep_b = NA_real_,
        N_varicella = NA_real_,
        N_personal_exempt = NA_real_,
        N_medical_exempt = NA_real_,
        N_full_exempt = NA_real_,
        pct_full_exempt = NA_real_,
        pct_personal_exempt = pct_personal_exempt,
        pct_medical_exempt = pct_medical_exempt
      ) %>%
      transmute(
        time, geography, geography_name, grade,
        N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
        N_personal_exempt, N_medical_exempt, N_full_exempt,
        pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
        pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
        enrollment, unit, public_independent,
        pct_fully_immunized, pct_provisional
      )

    dir.create("standard", showWarnings = FALSE)
    vroom::vroom_write(data_out, "standard/data.csv.gz")
  }

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
