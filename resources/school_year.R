# Canonical `time` for a school year, shared by the state ingest scripts.
#
# The standard output dates every observation to the START of the school year it
# covers, as YYYY-09-01: school year 2019-2020 is "2019-09-01". One date per
# school year is what lets the states be stacked and compared.
#
# Eleven ingests each invented their own instead, and they disagreed three ways:
#
#   * end-of-calendar-year   (ME, MI, ND, NY, SD, WV: "12-31-2020")
#     Dates the observation four months after the school year it describes, and
#     in MM-DD-YYYY, which does not sort.
#   * end-of-school-year     (MS, NM: "2020-12-31")
#     Same off-by-one-year, ISO-ordered.
#   * report/publication date (AL: "2021-01-27")
#     Dates it to when the state ran the report, so the same school year gets a
#     different date in every state, and a late-published year sorts out of
#     order.
#
# Both helpers take the year as an integer and return a character date, so a
# source that publishes "2019-2020" and one that publishes "SY20" land on the
# same value.

# Start year in hand: 2019 -> "2019-09-01".
school_year_time <- function(start_year) {
  y <- suppressWarnings(as.integer(start_year))
  if (any(is.na(y) & !is.na(start_year))) {
    stop("school_year_time(): not a year: ",
         paste(unique(start_year[is.na(y)]), collapse = ", "), call. = FALSE)
  }
  ifelse(is.na(y), NA_character_, sprintf("%04d-09-01", y))
}

# End year in hand -- the form most sources publish ("2019-2020", "Report
# Period ... 2020"): 2020 -> "2019-09-01".
school_year_time_from_end <- function(end_year) {
  y <- suppressWarnings(as.integer(end_year))
  if (any(is.na(y) & !is.na(end_year))) {
    stop("school_year_time_from_end(): not a year: ",
         paste(unique(end_year[is.na(y)]), collapse = ", "), call. = FALSE)
  }
  school_year_time(y - 1L)
}

# The END year of a school-year label: "2018-2019" and "2018-19" both give 2019.
# NA for a label that carries no such range.
#
# Vectorised, which is the whole point of it living here. Utah and Vermont each
# had their own copy that indexed the str_match() matrix at [1, 3] and so
# returned the FIRST row's year for an entire column: Utah stamped all 615 rows
# with one school year when its `Year` column runs 2018-2019 through 2022-2023,
# and Vermont collapsed eight school years of county rows onto one date. Both
# looked like a single-year source and neither errored.
school_year_end_from_label <- function(label) {
  m <- stringr::str_match(as.character(label),
                          "(20\\d{2})\\s*[-/]\\s*(20\\d{2}|\\d{2})")
  start <- m[, 2]
  end <- m[, 3]
  end_full <- ifelse(is.na(end), NA_character_,
                     ifelse(nchar(end) == 2L, paste0(substr(start, 1, 2), end),
                            end))
  as.integer(end_full)
}
