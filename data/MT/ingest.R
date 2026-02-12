# =============================================================================
# MT - County Immunization Coverage (2020)
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

parse_table <- function(path) {
  d <- readxl::read_excel(path, sheet = 1, col_names = FALSE)
  idx <- which(d[[1]] == "County")
  if (length(idx) == 0) return(NULL)

  header <- as.character(d[idx[1], ])
  data_raw <- d[(idx[1] + 1):nrow(d), ]
  names(data_raw) <- header

  data_raw %>%
    filter(!is.na(County)) %>%
    transmute(
      county = str_to_title(str_trim(County)),
      n_assessed = readr::parse_number(as.character(`Number\r\nAssessed`)),
      pct_utd = parse_pct_points(`%\r\nUTD`),
      pct_dtap = parse_pct_points(`% W/\r\nDTaP4`),
      pct_polio = parse_pct_points(`% W/\r\nPolio 3`),
      pct_mmr = parse_pct_points(`% W/\r\nMMR 1`),
      pct_hib_utd = parse_pct_points(`% W/\r\nHib UTD`),
      pct_hep_b = parse_pct_points(`% W/\r\nHep B 3`),
      pct_varicella = parse_pct_points(`% W/\r\nVar 1`),
      pct_pcv_utd = parse_pct_points(`% W/\r\nPCV UTD`)
    )
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_path <- "./raw/2020 Immunization Coverage  (1).xlsx"
  data_tbl <- parse_table(raw_path)

  if (!is.null(data_tbl)) {
    all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
    fips_df <- all_fips %>%
      filter(state == "MT") %>%
      mutate(geography_name = gsub(" County$", "", geography_name))

    state_fips <- fips_df %>%
      filter(nchar(geography) == 2) %>%
      distinct(geography) %>%
      pull(geography)

    data_out <- data_tbl %>%
      left_join(
        fips_df %>% filter(nchar(geography) == 5),
        by = c("county" = "geography_name")
      ) %>%
      mutate(
        geography = if_else(is.na(geography), state_fips[1], geography),
        geography_name = county,
        grade = "Overall",
        time = as.Date("2020-09-01"),
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
        n_assessed, pct_utd, pct_hib_utd, pct_pcv_utd
      )

    dir.create("standard", showWarnings = FALSE)
    vroom::vroom_write(data_out, "standard/data.csv.gz")
  }

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
