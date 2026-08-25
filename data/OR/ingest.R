library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")

# =============================================================================
# OR - K-12 School Immunization Exemptions & Enrollment, School and County
# Source: Oregon Health Authority (OHA), School Immunization Coverage.
#   The statewide K-12 workbook is published at a fixed URL that OHA overwrites
#   each fall, so downloading it directly makes the series self-updating (no more
#   committed one-year snapshot). Every row is one school: "Agency" is the
#   county, "SiteName" the school, adjusted enrollment is the denominator, and
#   the workbook gives a count and percent exempt for each required vaccine plus
#   the share up to date on all of them and the share with a nonmedical
#   exemption from any vaccine. County rows are derived by summing the
#   school-level counts and recomputing shares from those sums, as MI does,
#   rather than averaging the schools' published percentages.
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

# Download to a temp file and only move it into raw/ once it is there and
# readable.
#
# download.file() writes straight to its destination and TRUNCATES it when the
# transfer fails, so `try(download.file(..., raw_path))` did not merely fail to
# refresh the snapshot -- it destroyed the copy already on disk. One blocked
# request from OHA's load balancer took out data/OR/raw/SchK-12.xlsx entirely and
# the ingest then errored with "`path` does not exist".
tmp <- tempfile(fileext = ".xlsx")
ok <- tryCatch({
  download.file(k12_url, tmp, mode = "wb", quiet = TRUE)
  # A block page or an error body downloads "successfully"; readxl reading it is
  # the check that matters.
  readxl::excel_sheets(tmp)
  TRUE
}, error = function(e) FALSE, warning = function(w) FALSE)

if (ok) {
  file.copy(tmp, raw_path, overwrite = TRUE)
} else if (file.exists(raw_path)) {
  message("OR: download failed; keeping the existing raw/SchK-12.xlsx")
} else {
  stop("OR: could not download ", k12_url,
       " and there is no raw/SchK-12.xlsx to fall back on.")
}
unlink(tmp)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

# Percent columns are printed as strings like "93.3%"; parse_number() strips
# the "%" and leaves the value on the percent-point scale write_standard()
# expects (from = "percent"), so no further rescaling is needed here.
parse_pct_points <- function(x) {
  readr::parse_number(as.character(x))
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # Sheet name carries the school-year end (e.g. "K-12 2025" -> the 2024-25
  # cohort), so it is dated to the September that year started.
  #
  # There is deliberately no fallback: the previous version substituted the
  # CURRENT calendar year when the sheet name did not parse, which dated the
  # data to whenever the script happened to run and made the output depend on
  # the clock. A naming change from OHA now stops the build instead.
  sheet_name <- readxl::excel_sheets(raw_path)[1]
  year_match <- str_extract(sheet_name, "\\d{4}")
  if (is.na(year_match)) {
    stop("No 4-digit school year in the OR sheet name '", sheet_name,
         "' -- check the workbook before dating this file.")
  }
  time <- as.Date(school_year_time_from_end(year_match))

  data_raw <- readxl::read_excel(raw_path, sheet = sheet_name)

  # Every row is one school for the same K-12 cohort; a second Grade value
  # would mean OHA changed the workbook's layout and rows for another cohort
  # are mixed in here uncounted.
  grade_levels <- unique(data_raw$Grade)
  if (length(grade_levels) != 1) {
    stop("Expected a single Grade level in the OR K-12 workbook, found: ",
         paste(grade_levels, collapse = ", "))
  }

  pick <- function(nm) {
    if (nm %in% names(data_raw)) data_raw[[nm]] else rep(NA_character_, nrow(data_raw))
  }

  # OHA reports "# Exemption: <antigen>" / "% Exemption: <antigen>" per
  # required vaccine. MMR1 and MMR2 are kept separate rather than folded into
  # one "mmr" measure, since they are distinct dose requirements in OHA's own
  # columns and nothing in the source ties their counts together.
  vaccines <- c(
    dtap = "DTaP/Tdap", polio = "Polio", varicella = "Varicella",
    mmr1 = "MMR1", mmr2 = "MMR2", hep_b = "HepB", hep_a = "HepA"
  )

  schools <- tibble(
    county = data_raw$Agency,
    school_name = data_raw$SiteName,
    N_enrolled = readr::parse_number(as.character(
      pick("# Documentation Required (Adjusted Enrollment)")
    )),
    N_complete = suppressWarnings(as.numeric(pick("# With All Vaccines Required"))),
    pct_complete = parse_pct_points(pick("% With All Vaccines Required")),
    N_personal_exempt = suppressWarnings(as.numeric(pick("# Nonmedical Exemptions Any Vaccines"))),
    pct_personal_exempt = parse_pct_points(pick("% Nonmedical Exemptions Any Vaccines"))
  )

  for (key in names(vaccines)) {
    antigen <- vaccines[[key]]
    schools[[paste0("N_", key, "_exempt")]] <-
      suppressWarnings(as.numeric(pick(paste0("# Exemption: ", antigen))))
    schools[[paste0("pct_", key, "_exempt")]] <-
      parse_pct_points(pick(paste0("% Exemption: ", antigen)))
  }

  schools <- schools %>%
    filter(!is.na(county), nchar(trimws(county)) > 0)

  # OHA reports by local public health authority, nearly all of which are
  # counties. "North Central" is the tri-county health district (Gilliam,
  # Sherman, Wasco) and has no FIPS of its own; dropped rather than kept with
  # geography = NA, so the geography column holds nothing but FIPS codes. This
  # excludes that district's schools from both the school- and county-level
  # output, since there is no per-county enrollment split to allocate them by.
  schools <- schools %>%
    join_county_fips("OR", drop = "^north central$") %>%
    mutate(time = time, grade = "K-12", type = "school")

  COUNT_COLS <- grep("^N_", names(schools), value = TRUE)

  # County totals, summed across every school in the county, with shares
  # recomputed from the summed counts rather than averaged from the schools'
  # published percentages -- the same derivation MI uses, so a large school
  # is not weighted the same as a small one.
  counties <- schools %>%
    group_by(time, geography_name, geography, grade) %>%
    summarize(across(all_of(COUNT_COLS), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
    mutate(
      type = "county",
      county = NA_character_,
      school_name = NA_character_,
      pct_complete = 100 * rate_from_counts(N_complete, N_enrolled),
      pct_personal_exempt = 100 * rate_from_counts(N_personal_exempt, N_enrolled)
    )

  for (key in names(vaccines)) {
    counties[[paste0("pct_", key, "_exempt")]] <-
      100 * rate_from_counts(counties[[paste0("N_", key, "_exempt")]], counties$N_enrolled)
  }

  data <- bind_rows(schools, counties) %>%
    select(time, geography_name, geography, type, school_name, grade, everything(), -county) %>%
    arrange(time, geography_name, type, school_name)

  message(sprintf(
    "OR: %d rows (%d school, %d county), school year %s",
    nrow(data), sum(data$type == "school"), sum(data$type == "county"),
    paste(sort(unique(data$time)), collapse = ", ")))

  dir.create("standard", showWarnings = FALSE)
  write_standard(data, "Oregon", "./standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
