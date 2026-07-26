library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/add_state_column.R")

# =============================================================================
# OR - K-12 School Immunization Coverage & Exemptions by County
# Source: Oregon Health Authority (OHA), School Immunization Coverage.
#   The statewide K-12 workbook is published at a fixed URL that OHA overwrites
#   each fall, so downloading it directly makes the series self-updating (no more
#   committed one-year snapshot). School-level rows carry an "Agency" column that
#   is the county, plus adjusted enrollment and per-antigen coverage/exemption
#   percentages, which we enrollment-weight up to the county.
#   K-12:      https://www.oregon.gov/oha/PH/PREVENTIONWELLNESS/VACCINESIMMUNIZATION/GETTINGIMMUNIZED/Documents/SchK-12.xlsx
#   Preschool: same directory, SchPreschool.xlsx (child-care; not ingested here)
# =============================================================================

# OHA serves the file behind an F5 load balancer; a browser UA avoids blocks.
options(HTTPUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

k12_url <- paste0(
  "https://www.oregon.gov/oha/PH/PREVENTIONWELLNESS/VACCINESIMMUNIZATION/",
  "GETTINGIMMUNIZED/Documents/SchK-12.xlsx"
)
dir.create("raw", showWarnings = FALSE)
raw_path <- "./raw/SchK-12.xlsx"
try(download.file(k12_url, raw_path, mode = "wb", quiet = TRUE), silent = TRUE)

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

  # Sheet name carries the school-year end (e.g. "K-12 2025" -> 2024-25 cohort);
  # fall back to the current year if OHA changes the naming.
  sheet_name <- readxl::excel_sheets(raw_path)[1]
  year_match <- str_extract(sheet_name, "\\d{4}")
  if (is.na(year_match)) year_match <- format(Sys.Date(), "%Y")
  time <- as.Date(paste0(year_match, "-09-01"))

  data_raw <- readxl::read_excel(raw_path, sheet = sheet_name)

  pick <- function(nm) {
    if (nm %in% names(data_raw)) data_raw[[nm]] else rep(NA_character_, nrow(data_raw))
  }

  data_out <- tibble(
      county = data_raw$Agency,
      enrollment = readr::parse_number(as.character(pick("# Documentation Required (Adjusted Enrollment)"))),
      pct_dtap = parse_pct_points(pick("% Vaccinated: DTaP/Tdap")),
      pct_polio = parse_pct_points(pick("% Vaccinated: Polio")),
      pct_mmr = parse_pct_points(pick("% Vaccinated: MMR2")),
      pct_hep_b = parse_pct_points(pick("% Vaccinated: HepB")),
      pct_varicella = parse_pct_points(pick("% Vaccinated: Varicella")),
      pct_personal_exempt = parse_pct_points(pick("% Nonmedical Exemptions Any Vaccines")),
      pct_medical_exempt = parse_pct_points(pick("% With Medical Exemption(s)"))
    ) %>%
    filter(!is.na(county), nchar(trimws(county)) > 0) %>%
    group_by(county) %>%
    summarize(
      across(
        c(pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
          pct_personal_exempt, pct_medical_exempt),
        ~ weighted.mean(.x, enrollment, na.rm = TRUE)
      ),
      .groups = "drop"
    ) %>%
    mutate(time = time)

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "OR") %>%
    mutate(geography_name = gsub(" County", "", geography_name))

  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)

  data_out <- data_out %>%
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
      pct_full_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt
    ) %>%
    arrange(time, geography_name)

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(add_state_column(data_out, "Oregon"), "./standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
