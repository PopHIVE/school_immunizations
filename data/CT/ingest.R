library(dcf)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(vroom)
source("../../resources/add_state_column.R")

# =============================================================================
# CT - County/County-equivalent Immunization & Exemption Rates (county-level)
# Source: CT Open Data (Socrata) dataset 8kid-pp5k, "County or County Equivalent
#   Immunizations and Exemption Rates by School Year, Grade, Vaccine, and School
#   Type". Pulled directly from the API so the source self-updates; the manual
#   .xlsx is no longer required. Covers 2012-13 .. 2025-26, grades Pre-K/K/7th.
# Note: CT reports by traditional county for older years and by the new Council
#   of Governments "planning regions" (county-equivalents) from ~2022 on; both
#   are present in all_fips and handled by the crosswalk below.
# =============================================================================

api_url <- "https://data.ct.gov/resource/8kid-pp5k.csv?$limit=50000"
dir.create("raw", showWarnings = FALSE)
raw_file <- "raw/ct_county_immunization_exemption_8kid-pp5k.csv"
try(download.file(api_url, raw_file, mode = "wb", quiet = TRUE), silent = TRUE)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", "csv", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw <- readr::read_csv(raw_file, show_col_types = FALSE, col_types = readr::cols(.default = "c"))

  numify <- function(x) suppressWarnings(as.numeric(x))
  # take the (constant-per-group) value, tolerant of NAs
  first_val <- function(x) {
    x <- numify(x)
    x <- x[!is.na(x)]
    if (length(x) == 0) NA_real_ else x[1]
  }

  # Data label -> all_fips geography_name
  ct_xwalk <- c(
    "Fairfield" = "Fairfield County", "Hartford" = "Hartford County",
    "Litchfield" = "Litchfield County", "Middlesex" = "Middlesex County",
    "New Haven" = "New Haven County", "New London" = "New London County",
    "Tolland" = "Tolland County", "Windham" = "Windham County",
    "Capitol" = "Capitol", "Greater Bridgeport" = "Greater Bridgeport",
    "Lower CT River Valley" = "Lower Connecticut River Valley",
    "Naugatuck Valley" = "Naugatuck Valley",
    "Northeast CT" = "Northeastern Connecticut",
    "Northwest Hills" = "Northwest Hills",
    "South Central" = "South Central Connecticut",
    "Southeastern CT" = "Southeastern Connecticut",
    "Western" = "Western Connecticut"
  )

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_ct <- all_fips %>%
    filter(state == "CT", nchar(geography) == 5) %>%
    select(geography, fips_name = geography_name)
  state_fips <- "09"

  vax_map <- c(DTaP = "dtap", Polio = "polio", MMR = "mmr",
               HepB = "hep_b", Varicella = "varicella")

  # ---- Per-vaccine coverage -> wide ----
  cov <- raw %>%
    filter(vaccine_series %in% names(vax_map)) %>%
    mutate(vkey = unname(vax_map[vaccine_series])) %>%
    select(school_year, county_equivalent, grade, vkey,
           percentage_vaccinated, total_vaccinated_count) %>%
    mutate(
      percentage_vaccinated = numify(percentage_vaccinated),
      total_vaccinated_count = numify(total_vaccinated_count)
    ) %>%
    pivot_wider(
      id_cols = c(school_year, county_equivalent, grade),
      names_from = vkey,
      values_from = c(percentage_vaccinated, total_vaccinated_count),
      values_fn = ~ suppressWarnings(max(.x, na.rm = TRUE))
    )
  # replace -Inf produced by all-NA max
  cov <- cov %>% mutate(across(where(is.numeric), ~ ifelse(is.infinite(.x), NA_real_, .x)))

  # ---- Exemptions (constant across vaccines within county-year-grade) ----
  exempt <- raw %>%
    group_by(school_year, county_equivalent, grade) %>%
    summarise(
      pct_personal_exempt = first_val(percentage_religious_exemption),
      N_personal_exempt   = first_val(religious_exemption_count),
      pct_medical_exempt  = first_val(percentage_medical_exemption),
      N_medical_exempt    = first_val(medical_exemption_count),
      .groups = "drop"
    )

  combined <- cov %>%
    left_join(exempt, by = c("school_year", "county_equivalent", "grade")) %>%
    mutate(
      end_year = str_extract(school_year, "\\d{4}$"),
      time = as.Date(paste0(end_year, "-09-01")),
      geography_name = county_equivalent,
      fips_name = unname(ct_xwalk[county_equivalent])
    ) %>%
    left_join(fips_ct, by = "fips_name") %>%
    mutate(
      geography = if_else(county_equivalent == "State" | is.na(geography), state_fips, geography),
      geography_name = if_else(county_equivalent == "State", "Connecticut", geography_name)
    )

  data_out <- combined %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap = total_vaccinated_count_dtap,
      N_polio = total_vaccinated_count_polio,
      N_mmr = total_vaccinated_count_mmr,
      N_hep_b = total_vaccinated_count_hep_b,
      N_varicella = total_vaccinated_count_varicella,
      N_personal_exempt,
      N_medical_exempt,
      N_full_exempt = N_personal_exempt + N_medical_exempt,
      pct_dtap = percentage_vaccinated_dtap,
      pct_polio = percentage_vaccinated_polio,
      pct_mmr = percentage_vaccinated_mmr,
      pct_hep_b = percentage_vaccinated_hep_b,
      pct_varicella = percentage_vaccinated_varicella,
      pct_personal_exempt,
      pct_medical_exempt,
      pct_full_exempt = pct_personal_exempt + pct_medical_exempt
    ) %>%
    filter(!is.na(time)) %>%
    arrange(time, geography_name, grade)

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(add_state_column(data_out, "Connecticut"), "./standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
