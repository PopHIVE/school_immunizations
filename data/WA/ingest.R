# =============================================================================
# WA - Vaccine Exemption Counts and Rates by County
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

parse_end_year <- function(year_str) {
  m <- str_match(year_str, "(20\\d{2})-(20\\d{2})")
  if (!is.na(m[1, 1])) return(as.integer(m[1, 3]))
  m2 <- str_match(year_str, "(20\\d{2})-(\\d{2})")
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


get_col <- function(df, name) {
  if (name %in% names(df)) return(df[[name]])
  return(NA)
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_path <- "./raw/Washington Vaccine Exemption.xlsx"
  sheets <- readxl::excel_sheets(raw_path)

  data_all <- bind_rows(lapply(sheets, function(sh) {
    grade <- case_when(
      str_detect(tolower(sh), "kindergarten") ~ "Kindergarten",
      str_detect(tolower(sh), "7th") ~ "7th grade",
      str_detect(tolower(sh), "k-12") ~ "K-12",
      TRUE ~ sh
    )

    d <- readxl::read_excel(raw_path, sheet = sh)

    d %>%
      transmute(
        county = str_to_title(str_trim(County)),
        school_year = `School Year`,
        end_year = parse_end_year(`School Year`),
        time = as.Date(paste0(end_year, "-09-01")),
        enrollment = readr::parse_number(as.character(coalesce(
          get_col(d, "Total Enrollment"),
          get_col(d, "Enrollment")
        ))),
        n_full_exempt = readr::parse_number(as.character(get_col(d, "Exempt Count"))),
        pct_full_exempt = parse_pct_points(coalesce(
          get_col(d, "Exemption %...5"),
          get_col(d, "Exempt %")
        )),
        n_medical_exempt = readr::parse_number(as.character(get_col(d, "Medical Count"))),
        pct_medical_exempt = parse_pct_points(coalesce(
          get_col(d, "Exemption %...7"),
          get_col(d, "Medical %")
        )),
        n_personal_exempt = readr::parse_number(as.character(get_col(d, "Personal Count"))),
        pct_personal_exempt = parse_pct_points(get_col(d, "Personal %")),
        n_religious_exempt = readr::parse_number(as.character(get_col(d, "Religious Count"))),
        pct_religious_exempt = parse_pct_points(get_col(d, "Religious %")),
        n_religious_membership_exempt = readr::parse_number(as.character(get_col(d, "Religious Membership Count"))),
        pct_religious_membership_exempt = parse_pct_points(get_col(d, "Religious Membership %")),
        grade = grade
      )
  }))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "WA") %>%
    mutate(geography_name = gsub(" County$", "", geography_name))

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
      N_full_exempt = n_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      enrollment,
      n_religious_exempt, pct_religious_exempt,
      n_religious_membership_exempt, pct_religious_membership_exempt
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
