source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")
# =============================================================================
# NY - School Immunization Survey (School-Level)
# Source: Health Data NY (Socrata), two datasets that together cover 2012-13
#   onward, one row per school per survey year:
#   btkd-y8bp  School Immunization Survey: Beginning 2019-20 School Year
#     https://health.data.ny.gov/Health/School-Immunization-Survey-Beginning-2019-20-Schoo/btkd-y8bp
#     Refreshed by the state each year; pulled on every run.
#   5pme-xbs5  School Immunization Survey: 2012-13 through 2018-19
#     https://health.data.ny.gov/Health/School-Immunization-Survey-From-School-Year-2012-2/5pme-xbs5
#     A closed series, so it is fetched once and then kept as is.
# Both are CSV exports of the open-data API; the manual .xlsx download is no
# longer required.
#
# The two files differ in three ways the parse below reconciles:
#   * The older one carries a religious-exemption share (New York repealed
#     religious exemptions in June 2019, so the newer one has none) and lacks
#     Tdap and meningococcal, which were added to the survey in 2019-20.
#   * "District Name" / "Coordinates" in the older file are "District" /
#     "Location" in the newer one.
#   * They are not on the same scale -- see the scale note before the parse.
# =============================================================================

library(dplyr)
library(readr)
library(stringr)
library(vroom)

sources <- read_sources()
process <- dcf::dcf_process_record()
prev <- process$fetch_state

dir.create("raw", showWarnings = FALSE)

# The 2019-20-onward file used to be download.file()'d straight onto raw_file,
# and download.file() truncates its destination when the transfer fails, so a
# failed refresh destroyed the committed copy (the incident data/OR/ingest.R
# documents). fetch_file() downloads to a temporary file, checks that it is a
# CSV with the survey's header, and only then copies it over the committed one;
# a failure warns and leaves raw/ alone.
src_new <- source_entry(sources, "socrata_2019_on")
raw_file <- "raw/ny_school_immunization_survey.csv"
rec_new <- fetch_file(
  src_new$url, raw_file, type = "csv",
  expect_cols = c("School ID", "Report Period"),
  previous = prev[[raw_file]]
)

# 2012-13 .. 2018-19 is a closed series: once the file is on disk and
# validates it is not fetched again.
src_old <- source_entry(sources, "socrata_2012_2018")
raw_file_old <- "raw/ny_school_immunization_survey_2012_2018.csv"
rec_old <- fetch_file(
  src_old$url, raw_file_old, type = "csv",
  expect_cols = c("School ID", "Report Period", "Percent Religious Exemptions"),
  if_exists = "skip", previous = prev[[raw_file_old]]
)
process <- record_fetch(process, list(rec_new, rec_old))

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

# Gated on the script as well as the data, like every other state: the API
# returns a byte-identical file most runs, so without this an edit to the
# parsing below would never be applied to standard/.
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  # Both files are read as character: the survey prints its shares with a
  # fixed number of decimals ("0.99", "95.9"), and reading them as text keeps
  # the scale decision below explicit rather than something the reader did.
  read_survey <- function(path) {
    readr::read_csv(path, show_col_types = FALSE,
                    col_types = readr::cols(.default = "c"))
  }
  raw_new <- read_survey(raw_file)
  raw_old <- read_survey(raw_file_old) %>%
    rename(District = `District Name`, Location = Coordinates)

  # ---- Scale --------------------------------------------------------------------
  # Despite the "Percent ..." column names, the 2019-20-onward file publishes
  # PROPORTIONS: a school with full polio coverage reads 1.00, not 100. So it is
  # declared "rate" for every column. Declaring "percent" divided every value
  # by 100 and put the whole state out by two orders of magnitude
  # (rate_immunized_polio = 0.0099 for a school at 99%).
  #
  # The 2012-19 file is on two scales at once. Its "Percent Immunized ..." and
  # "Percent Completely Immunized" columns are PERCENT POINTS (a school at
  # full coverage reads 100; the median is 99.2 and no value lies in (0, 1]),
  # while its two exemption columns are PROPORTIONS (Pleasant View School,
  # 2012-13, reads 0 immunized against a religious exemption of 1 -- every
  # pupil exempt -- and a school at 98.5 completely immunized carries 0.0036,
  # not 0.36). So each column of that file is declared on its own, and both
  # files are converted to the 0-1 scale here, before they are stacked, so the
  # bound frame has one scale per column.
  #
  # The old `if_else(.x <= 1.5, .x * 100, .x)` rescaled per element, which
  # turned a school genuinely reporting 0.012 (1.2%) into 1.2 -- and the range
  # rule below then let it through as a plausible-looking share.
  #
  # Out-of-range values are dropped in both directions: the survey contains a
  # Tdap figure of -26 (Yaldeinu School, 2024), and a proportion cannot be
  # negative or above 1.
  to_rate <- function(d, scales) {
    d <- as_rate_columns(d, from = scales)
    d %>% mutate(across(
      starts_with("rate_"),
      ~ if_else(!is.na(.x) & (.x < 0 | .x > 1), NA_real_, .x)
    ))
  }

  # "Percent Medical Exemptions" -> pct_medical_exemptions, "Percent Immunized
  # Hepatitis B" -> pct_immunized_hepatitis_b, and so on. The 2012-19 file
  # spells one of them "HepatitisB" with no space; it is put on the newer
  # file's spelling first so the two land in the same column.
  pct_name <- function(x) {
    x <- sub("HepatitisB", "Hepatitis B", x, fixed = TRUE)
    str_replace_all(tolower(x), c("percent " = "pct_", " " = "_", "/" = "_"))
  }
  # The measure dictionary already names the religious share
  # rate_religious_exempt, so the old file's column goes there instead of
  # adding a rate_religious_exemptions spelling alongside it.
  rename_measures <- function(d) {
    pct_cols <- names(d)[grepl("^Percent ", names(d))]
    d <- rename_with(d, pct_name, all_of(pct_cols))
    if ("pct_religious_exemptions" %in% names(d)) {
      d <- rename(d, pct_religious_exempt = pct_religious_exemptions)
    }
    d
  }

  new_rates <- rename_measures(raw_new)
  new_scales <- setNames(rep("rate", sum(grepl("^pct_", names(new_rates)))),
                         grep("^pct_", names(new_rates), value = TRUE))
  new_rates <- to_rate(new_rates, new_scales)

  old_rates <- rename_measures(raw_old)
  old_pct <- grep("^pct_", names(old_rates), value = TRUE)
  old_scales <- setNames(
    ifelse(old_pct %in% c("pct_medical_exemptions", "pct_religious_exempt"),
           "rate", "percent"),
    old_pct
  )
  old_rates <- to_rate(old_rates, old_scales)

  # The county label is spelt three ways in the 2012-19 file that the newer
  # file does not use: "CHATAUQUA" (Chautauqua, 2 rows), "ST LAWRENCE"
  # without the stop (2 rows), and "ERIE physica" (Erie, 1 row -- a truncated
  # "physical address" note). The first and last are genuine misspellings
  # that no normalisation resolves, so they are recoded before the join.
  old_rates <- old_rates %>%
    mutate(County = recode(County,
      "CHATAUQUA" = "CHAUTAUQUA", "ST LAWRENCE" = "ST. LAWRENCE",
      "ERIE physica" = "ERIE"
    ))

  data <- bind_rows(old_rates, new_rates) %>%
    mutate(
      end_year = as.integer(str_sub(`Report Period`, -4, -1)),
      time = school_year_time_from_end(end_year)
    )

  # County FIPS through join_county_fips(), which matches on a normalised key
  # and stops on any label it cannot account for. The 2019-20-onward file
  # spells 141 rows "St.Lawrence" (no space), which a literal join against
  # "St. Lawrence" missed and silently dropped; the normalised key resolves
  # it. Two kinds of label carry no county and are dropped, with the count of
  # each reported: "ERROR: #N/A" (a spreadsheet lookup failure in the source,
  # 8 rows) and blank (3 rows). Any other unmatched label is an error.
  n_error <- sum(grepl("^ERROR", data$County, ignore.case = TRUE))
  n_blank <- sum(is.na(data$County) | trimws(data$County) == "")
  data <- join_county_fips(
    data, "NY", county_col = "County",
    drop = "^ERROR", drop_na = TRUE
  )
  message(sprintf(
    "NY: dropped %d row(s) with county 'ERROR: #N/A' and %d with a blank county",
    n_error, n_blank))

  data_out <- data %>%
    mutate(school_id = as.character(`School ID`)) %>%
    select(
      time,
      geography,
      geography_name,
      school_id,
      school_name = `School Name`,
      district = District,
      school_type = Type,
      rate_medical_exemptions,
      any_of("rate_religious_exempt"),
      starts_with("rate_immunized_"),
      rate_completely_immunized
    ) %>%
    # The Socrata CSV export returns its rows in a different order on every
    # download (the September 2026 refresh had the same 31,103 lines as the
    # committed file, shuffled), so the output is ordered here rather than
    # inheriting whichever order the export came in.
    arrange(time, geography, school_id, school_name)

  message(sprintf(
    "NY: %d school rows, %s to %s",
    nrow(data_out), min(data_out$time), max(data_out$time)))

  dir.create("standard", showWarnings = FALSE)
  # Every column is already on the 0-1 scale, so there is nothing left for
  # write_standard() to convert.
  out <- write_standard(data_out, "New York", "standard/data.csv.gz", from = "rate")
  update_latest_year(latest_school_year(out), ids = "socrata_2019_on")
  update_latest_year(
    latest_school_year(out %>% filter(as.integer(substr(time, 1, 4)) < 2019L)),
    ids = "socrata_2012_2018"
  )

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
