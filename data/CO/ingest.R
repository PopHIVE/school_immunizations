library(dcf)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")

# ---- Download from open-data API ----
# Source: CDPHE ArcGIS Open Data — Colorado School and Child Care Immunization
#   County Data (also surfaced on data.colorado.gov). Pulled directly so the
#   source self-updates; the manual CSV download is no longer required.
api_url <- paste0(
  "https://opendata.arcgis.com/api/v3/datasets/",
  "69a2917e89a0456fbe3f04dfb6767621_1/downloads/data?format=csv&spatialRefId=4326"
)
dir.create("raw", showWarnings = FALSE)
raw_path <- "./raw/CDPHE_Colorado_School_and_Child_Care_Immunization_County_Data_.csv"
try(
  download.file(api_url, raw_path, mode = "wb", quiet = TRUE),
  silent = TRUE
)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", "csv", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  data_raw <- readr::read_csv(raw_path, show_col_types = FALSE)
  # The live API names this column "Year"; older manual exports used "Year_".
  if ("Year" %in% names(data_raw) && !("Year_" %in% names(data_raw))) {
    data_raw <- dplyr::rename(data_raw, Year_ = Year)
  }
  data_raw <- data_raw %>%
    select(-OBJECTID)
  
  # CDPHE's `Metric` is folded into the COLUMN NAMES, not carried as a column.
  # The output is fully wide: one row per county/year/grade, and one column per
  # vaccine x metric, so a column means the same thing on every row of the file.
  #
  # Carrying `metric` as a column instead would make rate_mmr coverage on a
  # "fully_immunized" row and an MMR exemption rate on the other two -- a column
  # whose meaning depends on the row cannot be described by one piece of
  # metadata, and it stacks coverage on top of exemptions in one series for
  # anyone who does not think to filter on the metric.
  #
  # The two naming halves are deliberately different:
  #
  #   Fully Immunized -> rate_<vaccine>                    (e.g. rate_mmr)
  #   the exemptions  -> rate_<vaccine>_<metric>           (rate_mmr_medical_exempt)
  #
  # "Fully Immunized" per vaccine IS that vaccine's coverage, so it takes the
  # canonical coverage name the measure dictionary already defines and every
  # other state already uses -- rate_mmr means the same thing in Colorado as in
  # Nevada. Inventing rate_mmr_fully_immunized alongside it would give coverage
  # two spellings and let their descriptions drift apart.
  #
  # The Metric labels changed over time: older manual exports ran the words
  # together, the live API spaces them. Both spellings map to the same key.
  METRIC_KEYS <- c(
    "MedicalExemptions"    = "medical_exempt",
    "Medical Exemption"    = "medical_exempt",
    "NonMedicalExemptions" = "nonmedical_exempt",
    "Nonmedical Exemption" = "nonmedical_exempt",
    "FullyImmunized"       = "coverage",
    "Fully Immunized"      = "coverage"
  )

  # Column order for the measures. pivot_wider() emits them in first-appearance
  # order, which interleaves them arbitrarily -- rate_hep_b_nonmedical_exempt,
  # rate_pcv, rate_dtap, rate_covid_medical_exempt, ... Ordering by vaccine
  # instead keeps a vaccine's coverage and its two exemption rates together.
  # Anything not listed still survives, via everything() below.
  VACCINE_ORDER <- c("mmr", "dtap", "tdap", "polio", "hep_b", "varicella",
                     "hib", "pcv", "covid")

  # Not ingested: "In Process", "Incomplete Record" and "No Record" (the
  # non-compliance breakdown), and "Summary Compliant" (an all-vaccine
  # compliance rate, published with a blank Vaccine so it does not fit the
  # per-vaccine columns below). All four remain in raw/.
  data_base <- data_raw %>%
    filter(Metric %in% names(METRIC_KEYS)) %>%
    mutate(
      year_end = str_extract(Year_, "\\d{4}$"),
      time = as.Date(school_year_time_from_end(year_end)),
      grade = Survey_Type,
      vaccine_key = tolower(Vaccine),
      vaccine_key = gsub("[^a-z0-9]+", "_", vaccine_key),
      vaccine_key = gsub("_+", "_", vaccine_key),
      # CDPHE's raw label is "HepB" (no separator), which survives the regex
      # above as "hepb" -- align it to the "hep_b" spelling every other state
      # uses for this vaccine.
      vaccine_key = if_else(vaccine_key == "hepb", "hep_b", vaccine_key),
      metric_key = unname(METRIC_KEYS[Metric]),
      value = as.numeric(Value_Percent)
    ) %>%
    # Every metric kept above is published per vaccine, so a blank vaccine here
    # would be a parser failure rather than a real row.
    filter(!is.na(time), !is.na(vaccine_key), vaccine_key != "")

  data_wide <- data_base %>%
    mutate(col_name = if_else(
      metric_key == "coverage",
      paste0("pct_", vaccine_key),
      paste0("pct_", vaccine_key, "_", metric_key)
    )) %>%
    select(County, time, grade, col_name, value) %>%
    pivot_wider(
      names_from = col_name,
      values_from = value,
      # CDPHE duplicates Larimer County's 2022/2023 Tdap rows: every metric
      # appears twice, once with the published figure and once with 0. max()
      # keeps the figure. It cannot produce -Inf here -- Value_Percent is
      # populated on every row of this file.
      values_fn = list(value = function(x) max(x, na.rm = TRUE))
    )

  # CDPHE reports enrolment PER VACCINE, not per county-year-grade: in Adams
  # County 2021/2022 the Tdap rows carry 47,061 (the grades Tdap is required in)
  # while the K-12 vaccines carry 85,200. There is therefore no single
  # N_enrolled for a row, and folding the two together would mis-scale one of
  # them, so each vaccine's denominator is kept under its own name.
  #
  # It does NOT vary by metric, though -- CDPHE repeats the same denominator on
  # each metric row for a given county/year/grade/vaccine (checked: no group
  # carries more than one value) -- so distinct() collapses the three metric rows
  # to one and each vaccine gets a single denominator column, shared by that
  # vaccine's coverage and two exemption columns.
  data_enroll <- data_base %>%
    distinct(County, time, grade, vaccine_key, Enrollment) %>%
    mutate(col_name = paste0("N_", vaccine_key, "_enrolled")) %>%
    select(-vaccine_key) %>%
    pivot_wider(
      names_from = col_name,
      values_from = Enrollment,
      values_fn = list(Enrollment = function(x) max(x, na.rm = TRUE))
    )

  data_measures <- left_join(data_wide, data_enroll,
                             by = c("County", "time", "grade"))

  # The live API returns county names in upper case ("EL PASO") and includes an
  # "Unknown" bucket for records with no county; join_county_fips() folds the
  # casing away and requires both to be declared rather than guessed at.
  data_out <- data_measures %>%
    rename(county = County) %>%
    # Title-case only so the "Unknown" bucket keeps a readable label; county
    # matching itself is case-insensitive.
    mutate(county = str_to_title(county)) %>%
    # "Unknown" is dropped rather than kept with geography = NA: the standard
    # output carries only FIPS-coded geographies, and these 12 rows (records
    # CDPHE could not assign to a county) are in neither a county nor the
    # statewide total. They remain in raw/ if the bucket is ever needed.
    join_county_fips(
      "CO",
      statewide = c("State Total", "State Totals", "Total"),
      drop = "^unknown$"
    ) %>%
    # join_county_fips() already emits the canonical geography_name, so the raw
    # `county` label it was resolved from is a duplicate of it.
    select(-county) %>%
    # Index columns first, then the measures grouped by vaccine. Named on the
    # pct_ spelling because write_standard() renames pct_ -> rate_ afterwards,
    # preserving this order.
    #
    # The block of `pct_mmr = NA_real_, pct_dtap = NA_real_, ...` placeholders
    # that used to sit here is gone: pct_mmr and its siblings are now real
    # columns carrying the "Fully Immunized" figures, so stamping them NA would
    # overwrite the pivot. The overall-exemption placeholders (pct_full_exempt,
    # pct_personal_exempt, N_medical_exempt, ...) went with them -- CDPHE
    # publishes nothing that fills them, and drop_empty_measures() deleted all
    # sixteen on every run anyway.
    select(
      time, grade, geography, geography_name,
      any_of(as.vector(rbind(
        paste0("pct_", VACCINE_ORDER),
        paste0("pct_", VACCINE_ORDER, "_medical_exempt"),
        paste0("pct_", VACCINE_ORDER, "_nonmedical_exempt")
      ))),
      any_of(paste0("N_", VACCINE_ORDER, "_enrolled")),
      # A vaccine CDPHE adds later is not in VACCINE_ORDER and would be dropped
      # silently by a bare select; this keeps it, at the end.
      everything()
    )

  write_standard(data_out, "Colorado", "./standard/data.csv.gz", from = "percent")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
