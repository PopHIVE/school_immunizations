source("../../resources/rate_scale.R")
source("../../resources/county_fips.R")
source("../../resources/school_year.R")
#
# Download
#

# add files to the `raw` directory

#
# Reformat
#

# read from the `raw` directory, and write to the `standard` directory

# --- activate renv no matter where this script is run from ---

# Find project root by walking up until we see renv.lock

library(dcf)
library(tidyverse)
library(readxl)
library(dplyr)
library(stringr)
library(vroom)
library(readr)

## change here the 2 digit code being processed here
select.state = 'AL'

# check raw state. "~$..." files are Excel's lock files, present only while a
# workbook is open in Excel: they are not data, they are not readable as xlsx,
# and hashing them made the raw state depend on whether someone had the file
# open.
raw_workbooks <- function() {
  files <- list.files("raw", "\\.xlsx$", recursive = TRUE, full.names = TRUE)
  files[!grepl("^~\\$", basename(files))]
}
raw_state <- as.list(tools::md5sum(raw_workbooks()))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

# process raw if state has changed
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  # ---- helpers ----
  # ADPH publishes these as Excel-formatted proportions (0-0.04), which is
  # already the standard rate scale. The scale is declared, not inferred: the
  # previous per-element `ifelse(x > 1, x, x * 100)` rescaled only the cells at
  # or below 1, so a column containing both 0.98 and 1.63 came out with those
  # two values on different scales.
  parse_pct_points <- function(x) parse_rate(x, from = "rate")

  parse_num <- function(x) {
    if (is.numeric(x)) return(as.numeric(x))
    readr::parse_number(as.character(x))
  }

  # A blank county cell marks the statewide summary row. Case and punctuation
  # are left alone here -- join_county_fips() folds those away, which the old
  # str_to_title() pass did not: it produced "St.clair" from "ST.CLAIR" and
  # then failed its own "^St\\.?\\s*Clair$" fixup on the lowercased "c".
  normalize_county_name <- function(x) {
    out <- str_squish(as.character(x))
    if_else(is.na(out) | out == "" | tolower(out) == "na", "Total", out)
  }
  
  infer_grade <- function(path) {
    fn <- tolower(basename(path))
    if (str_detect(fn, "kindergarten")) return("Kindergarten")
    if (str_detect(fn, "seventh"))      return("7th grade")
    if (str_detect(fn, "ninth"))        return("9th grade")
    NA_character_
  }
  
  xlsx_files <- raw_workbooks()
  
  # ADPH names each sheet for the date it ran the report, not for the school
  # year the report covers, so the sheet name cannot be used as `time` directly:
  # the Kindergarten workbook's third report is dated 09.14.2023 and its fourth
  # 04.05.2024, which a month-based rule would put in the same school year.
  #
  # The workbook filename carries the range instead ("...2021-2025"), and each
  # workbook holds exactly one report per school year in chronological order, so
  # the i-th sheet is the school year ending in (first year of range + i - 1).
  # The count is asserted rather than assumed -- a workbook that gains a sheet
  # breaks the build instead of shifting every year by one.
  sheet_school_years <- function(xlsx_path, sheets) {
    rng <- str_match(basename(xlsx_path), "(\\d{4})\\s*-\\s*(\\d{4})")
    if (is.na(rng[1, 1])) {
      stop("No YYYY-YYYY school-year range in filename: ", basename(xlsx_path))
    }
    end_years <- as.integer(rng[1, 2]):as.integer(rng[1, 3])
    if (length(sheets) != length(end_years)) {
      stop(sprintf(
        paste0("%s: %d sheet(s) but %d school year(s) in the filename range ",
               "%s-%s. The sheet-to-school-year mapping below assumes one ",
               "report per year -- check the workbook before changing it."),
        basename(xlsx_path), length(sheets), length(end_years),
        rng[1, 2], rng[1, 3]))
    }
    dates <- as.Date(sheets, format = "%m.%d.%Y")
    if (any(is.na(dates))) {
      stop("Sheet name is not mm.dd.yyyy: '",
           paste(sheets[is.na(dates)], collapse = "', '"), "' in ",
           basename(xlsx_path))
    }
    setNames(school_year_time_from_end(end_years[rank(dates)]), sheets)
  }

  process_one_workbook <- function(xlsx_path) {
    grade_label <- infer_grade(xlsx_path)
    if (is.na(grade_label)) stop("Could not infer grade from filename: ", basename(xlsx_path))

    sheets <- readxl::excel_sheets(xlsx_path)
    sheet_time <- sheet_school_years(xlsx_path, sheets)

    bind_rows(lapply(sheets, function(sh) {
      df <- readxl::read_excel(xlsx_path, sheet = sh)

      # Normalize headers (remove embedded line breaks / tabs)
      names(df) <- names(df) %>%
        str_replace_all("\\s+", " ") %>%
        str_trim()

      df %>%
        transmute(
          time = sheet_time[[sh]],
          geography_name = normalize_county_name(County),
          grade = grade_label,
          
          # The denominator of every percentage in the workbook: each published
          # "%" column equals its own "#" column divided by this one, to machine
          # precision, across every county and sheet. So this is the enrolment
          # count the rates are taken over, and it is carried through as
          # N_enrolled rather than dropped.
          N_enrolled = parse_num(`# of Students`),


          # ORIGINAL exemption component COUNTS (keep them)
          n_full_med  = parse_num(`# with Full Medical Exemption`),
          n_part_med  = parse_num(`# UTD with Partial Medical Exemption`),
          n_full_rel  = parse_num(`# with Full Religous Exemption`),
          n_part_rel  = parse_num(`# UTD with Partial Religious Exemption`),
          
          # ORIGINAL exemption component PCTS (keep them, as percent points)
          pct_full_med = parse_pct_points(`% with Full Medical Exemption`),
          pct_part_med = parse_pct_points(`% UTD with Partial Medical Exemtion`),
          pct_full_rel = parse_pct_points(`% with Full Religous Exemption`),
          pct_part_rel = parse_pct_points(`% UTD with Partial Religious Exemption`)
        )
    }))
  }
  
  data_all <- lapply(xlsx_files, process_one_workbook) %>%
    bind_rows() %>%
    join_county_fips("AL", county_col = "geography_name", statewide = "Total") %>%
    mutate(
      # --- The four published components, each under its own name -------------
      N_full_medical_exempt = n_full_med,
      pct_full_medical_exempt = pct_full_med,

      N_partial_medical_exempt_utd = n_part_med,
      pct_partial_medical_exempt_utd = pct_part_med,

      N_full_religious_exempt = n_full_rel,
      pct_full_religious_exempt = pct_full_rel,

      N_partial_religious_exempt_utd = n_part_rel,
      pct_partial_religious_exempt_utd = pct_part_rel,

      # --- Canonical exemption columns ---------------------------------------
      # ADPH publishes two exemption grounds, medical and religious, each split
      # into a full exemption and a partial "UTD with" status. The canonical
      # columns take the FULL exemption of each ground; the partial-UTD counts
      # stay in their own columns above, because a student who is up to date
      # under a partial exemption is not exempt from the schedule.
      N_medical_exempt = n_full_med,
      pct_medical_exempt = pct_full_med,

      N_religious_exempt = n_full_rel,
      pct_religious_exempt = pct_full_rel,

      # Total fully exempt, from the two grounds. ADPH tabulates full medical
      # and full religious as separate grounds for the same student population,
      # so they add; their sum reaches 5.8% of enrolment at most, consistent
      # with no overlap. The share is divided out of the counts rather than
      # taken from a published total (there isn't one), so it carries no scale
      # to declare.
      N_full_exempt = n_full_med + n_full_rel,
      pct_full_exempt = rate_from_counts(n_full_med + n_full_rel, N_enrolled)

      # NOT emitted: N_personal_exempt / pct_personal_exempt. Alabama has no
      # personal or philosophical exemption -- only medical and religious. The
      # previous version filled the personal columns with the full RELIGIOUS
      # count, which published the same numbers under a category the state does
      # not have, and also set N_full_exempt to religious alone.
      #
      # Nor the empty N_dtap/pct_dtap/... block that used to sit here: these
      # workbooks carry no per-vaccine coverage, and an all-NA column advertises
      # a measure the state does not publish (it also beat the real columns in
      # scripts/build_all_states_county_standard.R, which picked the first
      # column that existed rather than the first that had data).
    ) %>%
    # No rounding here: these are rates, so round(x, 2) collapsed every value
    # below 0.005 to 0 -- which is most of the column, since ADPH exemption
    # rates run 0-0.04. Source precision is kept instead.
    transmute(
      time, geography, geography_name, grade,
      N_enrolled,
      N_medical_exempt, N_religious_exempt, N_full_exempt,
      pct_medical_exempt, pct_religious_exempt, pct_full_exempt,

      # published components, kept alongside the canonical columns
      N_full_medical_exempt, pct_full_medical_exempt,
      N_partial_medical_exempt_utd, pct_partial_medical_exempt_utd,
      N_full_religious_exempt, pct_full_religious_exempt,
      N_partial_religious_exempt_utd, pct_partial_religious_exempt_utd
    )
  
  write_standard(data_all, "Alabama", "./standard/data.csv.gz", from = "rate")
  
  # record processed raw state
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}





