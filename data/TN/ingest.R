source("../../resources/add_state_column.R")
# =============================================================================
# TN - Kindergarten MMR Vaccination Coverage by County (2024-25)
# Source: TN Department of Health, Kindergarten Survey. TN DoH supplied the
#   county MMR-coverage workbook directly to PopHIVE (there is no clean public
#   download; the WaPo Tableau/PDF source is deliberately NOT used). The file is
#   committed under `raw/`; we also fetch the PopHIVE-hosted copy if it is
#   missing so CI can rebuild. The workbook has two columns only:
#     county       - Tennessee county name
#     percent_mmr  - % of kindergartners fully immunized for MMR
#   so this source contributes KG / MMR only (no other antigens, no exemptions,
#   single 2024-25 cohort). See ../DATA_SOURCES.md.
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readxl)
library(vroom)

dir.create("raw", showWarnings = FALSE)
raw_path <- "raw/KMMRCoverage_County.xlsx"

# The file is normally committed to raw/; fetch the PopHIVE copy if absent.
if (!file.exists(raw_path)) {
  src <- paste0(
    "https://raw.githubusercontent.com/PopHIVE/Ingest/main/",
    "data/schoolvax_washpost/raw/KMMRCoverage_County.xlsx"
  )
  try(download.file(src, raw_path, mode = "wb", quiet = TRUE), silent = TRUE)
}

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # ---- Read the county MMR workbook -----------------------------------------
  tn_raw <- readxl::read_excel(raw_path, sheet = "Data") %>%
    as.data.frame()

  # ---- TN county FIPS (state FIPS "47"); match on lowercased bare name -------
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_tn <- all_fips %>%
    filter(state == "TN", nchar(geography) == 5) %>%
    mutate(county_key = tolower(sub(" County$", "", geography_name))) %>%
    select(geography, geography_name, county_key)

  data_out <- tn_raw %>%
    mutate(county_key = tolower(trimws(county))) %>%
    left_join(fips_tn, by = "county_key") %>%
    mutate(
      time = as.Date("2024-09-01"),
      grade = "Kindergarten",
      county = geography_name,
      pct_mmr = round(suppressWarnings(as.numeric(percent_mmr)), 1)
    ) %>%
    filter(!is.na(geography)) %>%
    transmute(time, geography, county, grade, pct_mmr) %>%
    arrange(county)

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(add_state_column(data_out, "Tennessee"), "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
