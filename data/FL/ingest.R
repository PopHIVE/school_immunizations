library(dcf)
library(dplyr)
library(readr)
library(stringr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_exempt <- function(x) {
  x <- str_squish(as.character(x))
  # Keep suppressed values ("<5") as NA so we do not overstate exemptions.
  x <- if_else(str_detect(x, "^<"), NA_character_, x)
  suppressWarnings(as.numeric(str_replace_all(x, ",", "")))
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  combined_path <- "raw/Florida Vaccine Exemption Data (Scraped)/Florida Exemption Data (23 Counties).csv"
  fl_raw <- readr::read_csv(combined_path, show_col_types = FALSE)

  fl_clean <- fl_raw %>%
    transmute(
      county = str_squish(County),
      total_pop_4_18 = suppressWarnings(as.numeric(str_replace_all(TotalPop4_18yrs, ",", ""))),
      total_exempt_4_18 = parse_exempt(TotalExempt4_18yrs)
    ) %>%
    filter(!is.na(county), county != "") %>%
    group_by(county) %>%
    summarize(
      total_pop_4_18 = sum(total_pop_4_18, na.rm = TRUE),
      total_exempt_4_18 = sum(total_exempt_4_18, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      time = as.Date("2024-09-01"),
      pct_full_exempt = if_else(
        total_pop_4_18 > 0,
        (total_exempt_4_18 / total_pop_4_18) * 100,
        NA_real_
      )
    )

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fl_fips <- all_fips %>%
    filter(state == "FL", nchar(geography) == 5) %>%
    transmute(
      geography,
      geography_name
    )

  data_out <- fl_clean %>%
    left_join(fl_fips, by = c("county" = "geography_name")) %>%
    mutate(
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = total_exempt_4_18,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_
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
      pct_full_exempt,
      total_pop_4_18
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
