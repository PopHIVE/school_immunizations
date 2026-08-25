library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_path <- "./raw/2014-2025 immunization exemption by school district.xlsx"

  # The workbook is one wide sheet: two rows of banner text, then the real
  # header on row 3, then one row per school DISTRICT (263 of them, no total
  # row). It is not year-long -- the school year is in the COLUMN name, and
  # each year contributes three columns:
  #
  #   "2019-20 Students Exempted"                exemption count
  #   "2019-20 Total Student Count"              enrolment (denominator)
  #   "2019-20 Percentage of Exempted Students"  the state's own share
  #
  # Reading only the "Students Exempted" block, as this ingest used to, threw
  # away the denominator and left the exemption RATE missing for every year.
  data_raw <- readxl::read_excel(raw_path, skip = 2)

  measure_labels <- c(
    "Students Exempted" = "N_full_exempt",
    "Total Student Count" = "total_enrolled",
    "Percentage of Exempted Students" = "pct_source"
  )

  data_district <- data_raw %>%
    rename(lea = LEA, district = `District Name`) %>%
    # Every cell arrives as character because the withheld ones read "N/A"
    # (a district that did not exist that year), so clean_numeric() -- not
    # parse_number() -- is what turns them into numbers and NA.
    mutate(across(-c(lea, district), as.character)) %>%
    pivot_longer(
      cols = -c(lea, district),
      names_to = c("year_label", "measure"),
      names_pattern = "^(\\d{4}-\\d{2}) (.+)$",
      values_to = "value"
    ) %>%
    filter(!is.na(year_label), measure %in% names(measure_labels)) %>%
    mutate(
      measure = unname(measure_labels[measure]),
      value = clean_numeric(value),
      time = as.Date(school_year_time(str_sub(year_label, 1, 4)))
    ) %>%
    pivot_wider(names_from = measure, values_from = value)

  # The three column blocks are matched by year label, not by position, but
  # police the alignment anyway: the state's published share has to equal its
  # own numerator over its own denominator. A block read off by one year would
  # not survive this.
  check_rate_against_counts(
    data_district$pct_source, data_district$N_full_exempt,
    data_district$total_enrolled, label = "AR by-district"
  )

  # ---- Geography -------------------------------------------------------------
  #
  # One row per school DISTRICT per school year -- the level the source actually
  # publishes. No county or statewide aggregate is written here: the counts are
  # emitted as reported and rolled up by
  # scripts/build_all_states_county_standard.R, which is where every other
  # sub-county state's roll-up happens (Maine and Wisconsin by school, Maryland
  # by school, Utah by district). Aggregating in the ingest as well would put the
  # same arithmetic in two places and give the mega-sheet a published county row
  # and a rolled-up one for the same county.
  #
  # `geography` is nonetheless the county FIPS, following the Maine/Wisconsin
  # convention for sub-county rows (Utah puts its state FIPS on district rows,
  # but Utah publishes no counts, so nothing downstream ever tries to roll them
  # up; here the counts exist and a state FIPS would roll up into a statewide
  # total mislabelled as a county). LEA supplies it: the code is 7 digits and the
  # first two are Arkansas's county number, assigned in alphabetical order --
  # the same order the county FIPS codes were assigned -- so prefix n is the nth
  # AR county by FIPS (01 = Arkansas = 05001, 75 = Yell = 05149).
  #
  # That attributes each district WHOLLY to the county of its administrative
  # office. Arkansas school districts are not nested in counties, and a few
  # straddle a line ("Mulberry Pleasant View Bi County", "County Line"), so any
  # county figure derived from these rows covers the districts administered from
  # that county, not every pupil resident in it. The district rows themselves are
  # exactly as published.
  ar_counties <- vroom::vroom("../../resources/all_fips.csv.gz",
                              show_col_types = FALSE, progress = FALSE) %>%
    filter(state == "AR", nchar(geography) == 5) %>%
    arrange(geography) %>%
    transmute(county_index = row_number(), geography,
              county = str_remove(geography_name, " County$"))

  data_district <- data_district %>%
    mutate(county_index = as.integer(str_sub(lea, 1, 2)))

  # The prefix-is-the-county-number claim is an assumption about someone else's
  # coding scheme, so it is checked rather than trusted. A workbook that adds a
  # prefix outside 1:75 -- or renumbers -- breaks the build instead of filing
  # districts under the wrong county.
  bad_index <- setdiff(unique(data_district$county_index),
                       ar_counties$county_index)
  if (length(bad_index)) {
    stop("AR: LEA prefix(es) outside the 75 AR counties: ",
         paste(sort(bad_index), collapse = ", "), call. = FALSE)
  }

  data_district <- data_district %>%
    left_join(ar_counties, by = "county_index")

  # Second check, on the names rather than the numbers: a district titled after
  # a county has to land in that county. 15 of the 263 do (the rest are named
  # for a town), and all 15 agree -- so the mapping is confirmed against the
  # workbook's own labels, not just against a count of 75.
  named_county <- str_match(
    data_district$district,
    paste0("\\b(", paste(ar_counties$county, collapse = "|"), ") County\\b")
  )[, 2]
  mismatch <- !is.na(named_county) & named_county != data_district$county
  if (any(mismatch)) {
    stop("AR: district(s) titled after a county other than their LEA county: ",
         paste(unique(sprintf("%s (LEA %s -> %s)",
                              data_district$district[mismatch],
                              data_district$lea[mismatch],
                              data_district$county[mismatch])),
               collapse = "; "), call. = FALSE)
  }

  # A district-year the workbook withheld ("N/A": a district or charter not
  # operating that year) carries no count and no enrolment, so it is dropped
  # rather than written as a row of NAs -- keeping it would say the district
  # reported zero exemptions out of an unknown enrolment.
  withheld <- is.na(data_district$N_full_exempt) |
    is.na(data_district$total_enrolled)

  data_out <- data_district[!withheld, , drop = FALSE] %>%
    arrange(time, lea) %>%
    transmute(
      time,
      geography,
      geography_name = county,
      school_district = district,
      lea,
      grade = "Overall",
      N_full_exempt,
      # From the counts rather than the workbook's own percentage column: it is
      # the same number (check_rate_against_counts above proves it) and needs no
      # scale declared.
      pct_full_exempt = 100 * N_full_exempt / total_enrolled,
      total_enrolled
    )

  message(sprintf(
    paste0("Arkansas: %d district-year rows, %d districts, %d school years, ",
           "%d county FIPS; %d district-year(s) withheld by the source"),
    nrow(data_out), n_distinct(data_out$lea), n_distinct(data_out$time),
    n_distinct(data_out$geography), sum(withheld)
  ))

  write_standard(data_out, "Arkansas", "./standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
