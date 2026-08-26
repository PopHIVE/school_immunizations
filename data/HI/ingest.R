library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
library(tidyr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)

  COUNTIES <- c("HAWAII", "HONOLULU", "KAUAI", "MAUI")

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
        time = time
      ) %>%
      select(time, county, school_name, school_type, enrollment,
             N_personal_exempt, N_medical_exempt)
  }

  schools <- bind_rows(lapply(raw_files, parse_one))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips <- all_fips %>%
    filter(state == "HI", nchar(geography) == 5) %>%
    transmute(
      geography,
      geography_name = str_remove(geography_name, " County$"),
      county = str_to_upper(str_trim(geography_name))
    )

  schools <- schools %>%
    left_join(county_fips, by = "county") %>%
    mutate(
      geography_name = str_to_title(str_to_lower(geography_name)),
      type = "school",
      grade = "Overall",
      pct_personal_exempt = if_else(!is.na(enrollment) & enrollment > 0,
                                     N_personal_exempt / enrollment, NA_real_),
      pct_medical_exempt = if_else(!is.na(enrollment) & enrollment > 0,
                                    N_medical_exempt / enrollment, NA_real_)
    )

  # County totals, summed across every school in the county at that school
  # year. NA (not-reported) schools are excluded from the sum by na.rm, not
  # from the school-level rows above.
  counties <- schools %>%
    group_by(time, geography, geography_name) %>%
    summarize(
      enrollment = sum(enrollment, na.rm = TRUE),
      N_personal_exempt = sum(N_personal_exempt, na.rm = TRUE),
      N_medical_exempt = sum(N_medical_exempt, na.rm = TRUE),
      # Enrollment-weighted county rate, left on the 0-1 scale.
      pct_personal_exempt = if_else(enrollment > 0, N_personal_exempt / enrollment, NA_real_),
      pct_medical_exempt = if_else(enrollment > 0, N_medical_exempt / enrollment, NA_real_),
      .groups = "drop"
    ) %>%
    mutate(type = "county", grade = "Overall", school_name = NA_character_, school_type = NA_character_)

  # N_personal_exempt/N_medical_exempt are dropped from the output, not just
  # the placeholder DTaP/polio/MMR/hep B/varicella measures HI never reports:
  # HI publishes only the exemption RATE per school, so the count columns
  # above are back-computed as enrollment * rate and inherit both the
  # source's rounding and, at the county level, compounding across every
  # school in it. rate_personal_exempt/rate_medical_exempt are the actual
  # published values and are kept.
  data_out <- bind_rows(schools, counties) %>%
    select(-county) %>%
    mutate(
      N_full_exempt = NA_real_,
      pct_full_exempt = NA_real_
    ) %>%
    select(
      time, geography, geography_name, type, school_name, school_type, grade,
      enrollment, N_full_exempt,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt
    )

  message(sprintf(
    "Hawaii: %d rows from %d workbooks (%d school, %d county)",
    nrow(data_out), length(raw_files),
    sum(data_out$type == "school"), sum(data_out$type == "county")))

  write_standard(data_out, "Hawaii", "./standard/data.csv.gz", from = "rate")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
