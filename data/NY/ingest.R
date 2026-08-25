source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
# =============================================================================
# NY - School Immunization Survey (School-Level)
# Source: Health Data NY (Socrata) dataset btkd-y8bp
#   https://health.data.ny.gov/Health/School-Immunization-Survey-Beginning-2019-20-Schoo/btkd-y8bp
# Pulled directly from the open-data API (CSV export) so the source
# self-updates; the manual .xlsx download is no longer required.
# =============================================================================

library(dplyr)
library(readr)
library(stringr)
library(vroom)

# ---- Download from open-data API ----
api_url <- "https://health.data.ny.gov/api/views/btkd-y8bp/rows.csv?accessType=DOWNLOAD"
dir.create("raw", showWarnings = FALSE)
raw_file <- "raw/ny_school_immunization_survey.csv"
try(
  download.file(api_url, raw_file, mode = "wb", quiet = TRUE),
  silent = TRUE
)

if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

raw_state <- list(hash = tools::md5sum(raw_file))
script_hash <- as.character(tools::md5sum("ingest.R"))

# Gated on the script as well as the data, like every other state: the API
# returns a byte-identical file most runs, so without this an edit to the
# parsing below would never be applied to standard/.
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  # Bare county names, matching join_county_fips() in the other states.
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "NY") %>%
    mutate(geography_name = sub(" County$", "", geography_name)) %>%
    select(geography, geography_name, state)

  raw <- readr::read_csv(raw_file, show_col_types = FALSE)

  percent_cols <- names(raw)[grepl("^Percent ", names(raw))]

  data <- raw %>%
    mutate(
      geography_name = str_to_title(str_trim(County)),
      end_year = as.integer(str_sub(`Report Period`, -4, -1)),
      time = school_year_time_from_end(end_year)
    ) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(state == "NY")

  # Despite the "Percent ..." column names, the survey publishes PROPORTIONS:
  # a school with full polio coverage reads 1.00, not 100. So `from = "rate"`
  # is declared at the write below. Declaring "percent" there divided every
  # value by 100 and put the whole state out by two orders of magnitude
  # (rate_immunized_polio = 0.0099 for a school at 99%).
  #
  # The old `if_else(.x <= 1.5, .x * 100, .x)` rescaled per element, which
  # turned a school genuinely reporting 0.012 (1.2%) into 1.2 -- and the range
  # rule below then let it through as a plausible-looking share.
  #
  # Out-of-range values are dropped in both directions: the survey contains a
  # Tdap figure of -26 (Yaldeinu School, 2024), and a proportion cannot be
  # negative or above 1.
  data <- data %>%
    mutate(
      across(
        all_of(percent_cols),
        ~ {
          v <- suppressWarnings(as.numeric(.x))
          if_else(!is.na(v) & (v < 0 | v > 1), NA_real_, v)
        }
      )
    )

  data_out <- data %>%
    rename_with(
      ~ str_replace_all(
        tolower(.x),
        c(
          "percent " = "pct_",
          " " = "_",
          "/" = "_"
        )
      ),
      all_of(percent_cols)
    ) %>%
    mutate(school_id = as.character(`School ID`)) %>%
    select(
      time,
      geography,
      geography_name,
      school_id,
      school_name = `School Name`,
      district = District,
      school_type = Type,
      starts_with("pct_")
    )

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "New York", "standard/data.csv.gz", from = "rate")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
