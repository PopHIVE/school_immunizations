library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
library(tidyr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")

sources <- read_sources()
src <- source_entry(sources, "doh_exemption_reports")
process <- dcf::dcf_process_record()
prev <- process$fetch_state

# DOH's report page lists one immunization/examination report per school
# year. Through 2023-24 they are PDFs only, and the workbooks in raw/ named
# "Hawaii <year> Vaccine Exemption.xlsx" were transcribed from them by hand.
# From 2024-25 DOH also posts the report as xlsx, which is what the pattern
# in sources.json matches; those are fetched into raw/ under their own names
# and parsed by parse_official() below. A posted year is never revised, so a
# file already on disk is not re-requested.
#
# There is no 2020-21 report on the page at all (it goes 2019-2020 PDF, then
# 2021-22 PDF), which is why that school year is absent from the output.
dir.create("raw", showWarnings = FALSE)
links <- discover_links(src$page_url, src$pattern, must_find = FALSE)
recs <- list()
if (nrow(links)) {
  recs <- fetch_many(links$url, dest_fn = function(u) file.path("raw", basename(u)),
                     type = "xlsx", if_exists = "skip", previous = prev)
}
process <- record_fetch(process, recs)

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)
  OFFICIAL_PATTERN <- "Immunization_Examination_Req_Report_for_School_Year_(\\d{2})_\\d{2}\\.xlsx$"
  official_files <- raw_files[grepl(OFFICIAL_PATTERN, basename(raw_files))]
  transcribed_files <- setdiff(raw_files, official_files)

  COUNTIES <- c("HAWAII", "HONOLULU", "KAUAI", "MAUI")

  # The transcribed workbooks write the school type in upper case ("PUBLIC")
  # and DOH's own file in title case ("Public"); the output carries one
  # spelling. A value outside the known set is set to NA and reported with
  # its count, so a shifted column or a stray fragment in a future workbook
  # shows up in the log rather than as a new category.
  SCHOOL_TYPES <- c("Public", "Private", "Charter", "DHS", "Day Care Center")
  normalize_school_type <- function(x, label) {
    x <- str_squish(as.character(x))
    out <- SCHOOL_TYPES[match(str_to_upper(x), str_to_upper(SCHOOL_TYPES))]
    bad <- !is.na(x) & x != "" & is.na(out)
    if (any(bad)) {
      message(sprintf(
        "HI: %s: %d row(s) with a school type outside %s set to NA: %s",
        label, sum(bad), paste(SCHOOL_TYPES, collapse = "/"),
        paste(unique(x[bad]), collapse = "; ")))
    }
    out
  }

  # Column layout is not stable across years. 2014-15 through 2018-19 order
  # (school, school type, island); 2019-20 on order (school, county, school
  # type) and append extra measure columns (Incomplete Immunizations, Missing
  # Physicals, ...) this ingest does not use. The layout can't be told apart
  # by row content -- the later years' county column sometimes holds a
  # specific island (Niihau, Molokai) rather than the county it belongs to,
  # which would be misread as a school type if detected per row -- so it is
  # read once per file from the header labels in row 1 instead.
  parse_one <- function(path) {
    data_raw <- readxl::read_excel(path, skip = 1, col_names = FALSE) %>%
      setNames(paste0("c", seq_len(ncol(.))))

    year_range <- str_extract(basename(path), "\\d{4}-\\d{2}")
    year_start <- str_extract(year_range, "^\\d{4}")
    time <- school_year_time(year_start)

    new_layout <- identical(str_to_lower(str_trim(as.character(data_raw$c2[[1]]))), "county")
    county_col <- if (new_layout) data_raw$c2 else data_raw$c3
    school_type_col <- if (new_layout) data_raw$c3 else data_raw$c2

    data_raw <- data_raw %>%
      mutate(
        c1 = as.character(c1),
        county_raw = as.character(county_col),
        school_type = str_trim(as.character(school_type_col)),
        c4 = as.character(c4),
        c5 = as.character(c5),
        c6 = as.character(c6)
      )

    # A handful of rows (e.g. 2021-22, Hawaii county) hold two schools' data
    # in one spreadsheet row, every field "\r\n"-joined -- an export artifact,
    # not a real merged cell. Split only when every relevant column breaks
    # into the SAME number of pieces as the school name; a name that merely
    # wraps onto two lines ("...Lab\r\nPCS") has no such split in the other
    # columns and stays one row, so it is not mistaken for a second school
    # with duplicated (and so double-counted) enrollment and exemption data.
    n_segments <- function(x) lengths(str_split(x, fixed("\r\n")))
    seg_counts <- n_segments(data_raw$c1)
    is_merged_row <- seg_counts > 1 &
      seg_counts == n_segments(data_raw$county_raw) &
      seg_counts == n_segments(data_raw$school_type) &
      seg_counts == n_segments(data_raw$c4) &
      seg_counts == n_segments(data_raw$c5) &
      seg_counts == n_segments(data_raw$c6)
    is_merged_row[is.na(is_merged_row)] <- FALSE

    if (any(is_merged_row)) {
      data_raw <- data_raw %>%
        mutate(.orig_row = row_number())
      data_raw <- bind_rows(
        data_raw[!is_merged_row, ],
        data_raw[is_merged_row, ] %>%
          separate_rows(c1, county_raw, school_type, c4, c5, c6, sep = "\r\n")
      ) %>%
        arrange(.orig_row) %>%
        select(-.orig_row)
    }

    data_raw %>%
      mutate(
        is_county_header = str_detect(str_to_upper(str_trim(c1)), "COUNTY$"),
        section_county = if_else(
          is_county_header,
          str_to_upper(str_trim(str_remove(c1, "\\s+COUNTY$"))),
          NA_character_
        )
      ) %>%
      tidyr::fill(section_county, .direction = "down") %>%
      mutate(
        county = if_else(
          str_to_upper(str_trim(county_raw)) %in% COUNTIES,
          str_to_upper(str_trim(county_raw)),
          section_county
        ),
        school_name = str_trim(c1),
        enrollment = readr::parse_number(
          c4,
          na = c("", "NA", "NR", "N/R", "DNR", "Enrollment", "Total Enrollment", "Total\r\nEnrollment")
        ),
        # Every workbook publishes these as bare proportions ("0.104200"),
        # so the per-element `if_else(x > 1, x / 100, x)` guard never fired --
        # except the two-school merge split above, whose cells are typed as
        # percent strings ("0.84%") instead of computed proportions. Detected
        # per cell, since that one convention sits beside the other 3,700+
        # in the same file.
        pct_religious = if_else(str_detect(c5, fixed("%")),
                                 parse_rate(c5, from = "percent"),
                                 parse_rate(c5, from = "rate")),
        pct_medical = if_else(str_detect(c6, fixed("%")),
                               parse_rate(c6, from = "percent"),
                               parse_rate(c6, from = "rate"))
      ) %>%
      filter(
        !is_county_header,
        !is.na(county),
        county %in% COUNTIES,
        str_to_lower(school_name) != "school name",
        !str_detect(str_to_upper(school_name), "ALL SCHOOLS"),
        # Footnotes and legend text below the last county's table (e.g.
        # "Definitions", "NR: Did not report...") inherit that county from
        # the same fill-down that carries a real trailing NR school, but
        # carry no data in ANY column -- a real school, reported or not,
        # always has a school type even when its counts are NR.
        !(is.na(school_type) & is.na(enrollment) & is.na(pct_religious) & is.na(pct_medical))
      ) %>%
      mutate(
        # A cell the source marked "N/R" (not reported) or "NR" parses to NA
        # above rather than being dropped here -- the school still gets a row,
        # just with no measurement for that year.
        N_personal_exempt = enrollment * pct_religious,
        N_medical_exempt = enrollment * pct_medical,
        school_type = normalize_school_type(school_type, basename(path)),
        time = time
      ) %>%
      select(time, county, school_name, school_type, enrollment,
             N_personal_exempt, N_medical_exempt)
  }

  # DOH's own xlsx (2024-25 on): one sheet, header in row 1 (School Name,
  # County, School Type, Enrollment, Religious Exemptions, Medical Exemptions,
  # No Immunization Record, Missing Immunizations, Total Not Up to Date,
  # Missing Physical Examinations), values as proportions. The first rows are
  # the four county totals ("HAWAII COUNTY (K-12)"), then statewide rows
  # ("HAWAII STATE - ALL SCHOOLS (K-12)", "(K)", "(7)"), then each county's
  # schools under a bare "HAWAII COUNTY" section label with no data. Every
  # school row carries its county in the County column, so no fill-down is
  # needed, and the county rows are DOH's own totals, so they are taken as
  # published rather than re-summed from the schools. The statewide rows are
  # not kept: the output has never carried a state row, and the combined
  # file derives its own totals. Missing Physical Examinations is not an
  # immunization measure and is not carried either.
  #
  # "Total Not Up to Date" is the sum of the two exemption shares, no record
  # and missing immunizations (checked against the county rows).
  parse_official <- function(path) {
    year_start <- 2000L + as.integer(str_match(basename(path), OFFICIAL_PATTERN)[, 2])
    time <- school_year_time(year_start)

    d <- readxl::read_excel(path, col_types = "text")
    expected <- c("School Name", "County", "School Type", "Enrollment",
                  "Religious Exemptions", "Medical Exemptions",
                  "No Immunization Record", "Missing Immunizations",
                  "Total Not Up to Date")
    missing <- setdiff(expected, names(d))
    if (length(missing)) {
      stop("HI: ", basename(path), " lacks column(s): ", paste(missing, collapse = ", "),
           call. = FALSE)
    }

    d %>%
      transmute(
        time = time,
        school_name = str_squish(`School Name`),
        county = str_to_upper(str_squish(County)),
        school_type = str_squish(`School Type`),
        enrollment = readr::parse_number(Enrollment, na = c("", "NA", "NR", "N/R", "DNR")),
        pct_personal_exempt = parse_rate(`Religious Exemptions`, from = "rate"),
        pct_medical_exempt = parse_rate(`Medical Exemptions`, from = "rate"),
        pct_no_record = parse_rate(`No Immunization Record`, from = "rate"),
        pct_missing_immunizations = parse_rate(`Missing Immunizations`, from = "rate"),
        pct_not_utd = parse_rate(`Total Not Up to Date`, from = "rate")
      ) %>%
      mutate(
        is_county_total = str_detect(school_name, "^[A-Z]+ COUNTY \\(K-12\\)$") &
          county %in% COUNTIES,
        is_state = str_detect(school_name, "^HAWAII STATE"),
        is_section = str_detect(school_name, "^[A-Z]+ COUNTY$") & is.na(county)
      ) %>%
      filter(!is.na(school_name), !is_state, !is_section) %>%
      mutate(
        type = if_else(is_county_total, "county", "school"),
        school_name = if_else(is_county_total, NA_character_, school_name),
        # County totals carry "-" for the type; blanked before the check so
        # it is not reported as an unknown value.
        school_type = if_else(is_county_total, NA_character_, school_type),
        school_type = normalize_school_type(school_type, basename(path))
      ) %>%
      select(-is_county_total, -is_state, -is_section)
  }

  schools <- bind_rows(lapply(transcribed_files, parse_one)) %>%
    mutate(
      type = "school",
      pct_personal_exempt = if_else(!is.na(enrollment) & enrollment > 0,
                                     N_personal_exempt / enrollment, NA_real_),
      pct_medical_exempt = if_else(!is.na(enrollment) & enrollment > 0,
                                    N_medical_exempt / enrollment, NA_real_)
    )

  # County totals for the transcribed years, summed across every school in
  # the county at that school year. NA (not-reported) schools are excluded
  # from the sum by na.rm, not from the school-level rows above.
  counties <- schools %>%
    group_by(time, county) %>%
    summarize(
      enrollment = sum(enrollment, na.rm = TRUE),
      N_personal_exempt = sum(N_personal_exempt, na.rm = TRUE),
      N_medical_exempt = sum(N_medical_exempt, na.rm = TRUE),
      # Enrollment-weighted county rate, left on the 0-1 scale.
      pct_personal_exempt = if_else(enrollment > 0, N_personal_exempt / enrollment, NA_real_),
      pct_medical_exempt = if_else(enrollment > 0, N_medical_exempt / enrollment, NA_real_),
      .groups = "drop"
    ) %>%
    mutate(type = "county", school_name = NA_character_, school_type = NA_character_)

  official <- bind_rows(lapply(official_files, parse_official))

  # N_personal_exempt/N_medical_exempt are dropped from the output: HI
  # publishes only the exemption RATE per school, so the count columns above
  # are back-computed as enrollment * rate and inherit both the source's
  # rounding and, at the county level, compounding across every school in
  # it. rate_personal_exempt/rate_medical_exempt are the actual published
  # values and are kept.
  data_out <- bind_rows(schools, counties, official) %>%
    mutate(grade = "Overall") %>%
    join_county_fips("HI") %>%
    select(
      time, geography, geography_name, type, school_name, school_type, grade,
      enrollment, pct_personal_exempt, pct_medical_exempt,
      pct_no_record, pct_missing_immunizations, pct_not_utd
    )

  message(sprintf(
    "Hawaii: %d rows from %d workbooks (%d school, %d county), school years %s",
    nrow(data_out), length(raw_files),
    sum(data_out$type == "school"), sum(data_out$type == "county"),
    paste(sort(unique(data_out$time)), collapse = ", ")))

  out <- write_standard(data_out, "Hawaii", "./standard/data.csv.gz", from = "rate")
  update_latest_year(latest_school_year(out))

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
