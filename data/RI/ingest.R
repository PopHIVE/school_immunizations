library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

# The workbook publishes proportions (0.0094 = 0.94%), so the scale is declared
# rather than inferred. A "<x" cell is a suppressed small cell: it now becomes
# NA, instead of the old x/2 substitution, which invented a value the source
# never reported.
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  ri_raw <- readxl::read_excel(
    "raw/RI_RE rates_Yale_Aug 2025 (1).xlsx",
    sheet = "Sheet1",
    col_names = FALSE
  )

  # Row 1 of the workbook reads "Religious exemption rates", and the per-vaccine
  # columns beneath it are exactly that -- the share of students with a
  # religious exemption for each series, not the share vaccinated. They are
  # named as exemptions accordingly; the coverage columns are left missing,
  # because RIDOH publishes no coverage here and 1 - exemption is not coverage
  # (a student can be unvaccinated without an exemption, and an exempt student
  # may still have had the vaccine).
  colnames(ri_raw)[1:9] <- c(
    "state_abbr", "school_year", "grade_raw",
    "pct_mmr_religious_exempt", "pct_dtap_religious_exempt",
    "pct_polio_religious_exempt", "pct_varicella_religious_exempt",
    "pct_menacwy_religious_exempt", "pct_hep_b_religious_exempt"
  )

  ri_clean <- ri_raw %>%
    slice(-(1:2)) %>%
    transmute(
      state_abbr = str_squish(state_abbr),
      school_year = str_squish(school_year),
      grade = case_when(
        str_to_lower(str_squish(grade_raw)) == "k" ~ "Kindergarten",
        TRUE ~ str_squish(grade_raw)
      ),
      start_year = str_extract(school_year, "^\\d{4}"),
      time = suppressWarnings(as.Date(paste0(start_year, "-09-01"), format = "%Y-%m-%d")),
      pct_mmr_religious_exempt =
        parse_rate(pct_mmr_religious_exempt, from = "rate"),
      pct_dtap_religious_exempt =
        parse_rate(pct_dtap_religious_exempt, from = "rate"),
      pct_polio_religious_exempt =
        parse_rate(pct_polio_religious_exempt, from = "rate"),
      pct_varicella_religious_exempt =
        parse_rate(pct_varicella_religious_exempt, from = "rate"),
      pct_menacwy_religious_exempt =
        parse_rate(pct_menacwy_religious_exempt, from = "rate"),
      pct_hep_b_religious_exempt =
        parse_rate(pct_hep_b_religious_exempt, from = "rate")
    ) %>%
    filter(!is.na(state_abbr), state_abbr != "", !is.na(time))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state, geography_name)

  data_out <- ri_clean %>%
    left_join(state_fips, by = c("state_abbr" = "state")) %>%
    mutate(
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      # Coverage: not published by this source (see the header note above).
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      # RIDOH reports religious exemptions per vaccine series only: no medical
      # or any-reason figure, and no all-series total.
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_,
      pct_full_exempt = NA_real_
    ) %>%
    filter(!is.na(geography)) %>%
    transmute(
      time,
      geography,
      geography_name,
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
      pct_dtap_religious_exempt,
      pct_polio_religious_exempt,
      pct_mmr_religious_exempt,
      pct_hep_b_religious_exempt,
      pct_varicella_religious_exempt,
      pct_menacwy_religious_exempt
    )

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "Rhode Island", "standard/data.csv.gz", from = "rate")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
