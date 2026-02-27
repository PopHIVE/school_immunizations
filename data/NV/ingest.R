library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_num <- function(x) {
  x <- as.character(x)
  x <- str_replace_all(x, ",", "")
  x <- str_remove(x, "^<\\s*")
  suppressWarnings(as.numeric(x))
}

as_pct_points <- function(x) {
  if (all(is.na(x))) return(x)
  if (max(x, na.rm = TRUE) <= 1) return(x * 100)
  x
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  nv_raw <- readxl::read_excel(
    "raw/2010-2024 MMR Kindergarten.xlsx",
    sheet = "Raw Data"
  )

  nv_clean <- nv_raw %>%
    transmute(
      county = str_squish(`School County`),
      school_year = str_remove(str_squish(`School Year`), "\\*$"),
      time = as.Date(paste0(str_extract(school_year, "^\\d{4}"), "-09-01")),
      pct_mmr = as_pct_points(parse_num(`Up to Date for MMR`)),
      pct_medical_exempt = as_pct_points(parse_num(`Medical MMR Exemptions`)),
      pct_personal_exempt = as_pct_points(parse_num(`Religious MMR Exemptions`))
    ) %>%
    mutate(
      pct_full_exempt = if_else(
        is.na(pct_medical_exempt) & is.na(pct_personal_exempt),
        NA_real_,
        coalesce(pct_medical_exempt, 0) + coalesce(pct_personal_exempt, 0)
      )
    ) %>%
    filter(!is.na(time), !is.na(county), county != "")

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  nv_fips <- all_fips %>%
    filter(state == "NV", nchar(geography) == 5) %>%
    transmute(
      geography,
      geography_name,
      county = geography_name %>%
        str_replace(" County$", "") %>%
        str_squish()
    )

  data_out <- nv_clean %>%
    left_join(nv_fips, by = "county") %>%
    mutate(
      grade = "Kindergarten",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_
    ) %>%
    filter(!is.na(geography)) %>%
    transmute(
      time,
      geography,
      geography_name = county,
      grade,
      N_dtap,
      N_polio,
      N_mmr,
      N_hep_b,
      N_varicella,
      N_personal_exempt,
      N_medical_exempt,
      N_full_exempt,
      pct_dtap,
      pct_polio,
      pct_mmr,
      pct_hep_b,
      pct_varicella,
      pct_personal_exempt,
      pct_medical_exempt,
      pct_full_exempt
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
