source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")
source("../../resources/pdf_table.R")
# =============================================================================
# TN - Kindergarten Immunization Compliance Assessment (Kindergarten Survey)
# Source: Tennessee Department of Health, Vaccine-Preventable Diseases and
# Immunization Program. Four sources, all from the annual kindergarten
# survey (see sources.json and README.md):
#
#   kindergarten_compliance_pdf
#     The annual Kindergarten Immunization Compliance Assessment Report, one
#     PDF per school year from 2019-20, discovered on
#     https://www.tn.gov/health/immunization.html and kept as
#     raw/kindergarten_compliance_<start year>.pdf. Two things are read
#     from each report:
#       * Appendix 1, Table 1: per county the percent of students fully
#         immunized, the number of students, and the number of schools in the
#         95-100%, 90-94.9% and <90% bands. Public and private schools
#         combined from 2020-21; the 2019-20 report gives public schools in
#         Table 1 and private schools in Table 2. The band columns are
#         printed <90 / 90-94 / 95-100 through 2022-23 and 95-100 / 90-94 /
#         <90 from 2023-24, so the order is read from the header positions.
#       * The public and private school statewide summaries: students fully
#         immunized (n of N) and the count and share in each not-fully-
#         immunized category (religious exemption, incomplete record, missing
#         record, temporary certificate, transfer within 30 days, medical
#         exemption). Rates are recomputed from the counts; the printed share
#         is only checked against them.
#     The county pages of Appendix 2 are images and are not read.
#
#   kindergarten_school_reports_pdf
#     For 2017-18 and 2018-19 the assessment was published as school
#     listings instead, one PDF each for public and private schools, kept
#     as raw/kindergarten_schools_<start year>_<public|private>.pdf. Every
#     school is listed with its enrollment and the count and share in each
#     category, with a COUNTY TOTAL row per county and a STATEWIDE TOTAL row
#     at the end. Only the total rows are read; they carry the same measures
#     as the later statewide summaries. The county name is printed once per
#     block of rows, vertically centred and usually on a line of its own,
#     so the total rows are matched to it by word position (pdf_words)
#     rather than by text line. The private reports have no transfer
#     column.
#
#   kindergarten_survey_csv
#     KINDERGARTEN_SURVEY.csv from the TDH coverage-rate dashboard: one row
#     per responding school with the same category counts, 2019-20 to
#     2021-22 as of September 2026. School rows go to
#     standard/data_schools.csv.gz and county sums by school type to
#     standard/data_survey_counties.csv.gz. They are kept apart from the
#     report tables because the file assigns some schools to a different
#     county than the 2019-20 report does (see README.md).
#
#   tdh_mmr_county
#     The 2024-25 county kindergarten MMR workbook TDH supplied to PopHIVE
#     (rate_mmr), joined onto the combined-school county row for 2024-25.
#
# raw/ holds page subsets of the report PDFs, not the files TDH posts. The
# compliance reports are 8-29 MB each (80 MB for six years), nearly all of
# it the Appendix 2 county images. fetch_report() downloads a report to a
# temporary file, runs the parser on it, and writes only the pages the
# parser read (the statewide summary sections and the Appendix 1 tables,
# 8 to 11 pages a year) to raw/ with qpdf::pdf_subset(). The subset is
# parsed again and must give the same result as the full file. A report
# whose subset is already in raw/ and parses is not downloaded again. The
# school listings are read page by page, so their subset is every table
# page; only the cover page with the category definitions is dropped.
# fetch_state in process.json describes the subsets (bytes, sha256).
#
# Rows carry `type` (county, state, school) and `school_type` (public,
# private, all). Rates are 0-1; counts are N_<measure>.
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readxl)
library(vroom)

sources <- read_sources()
mmr_src <- source_entry(sources, "tdh_mmr_county")
pdf_src <- source_entry(sources, "kindergarten_compliance_pdf")
sch_src <- source_entry(sources, "kindergarten_school_reports_pdf")
csv_src <- source_entry(sources, "kindergarten_survey_csv")

process <- dcf::dcf_process_record()
prev <- process$fetch_state

dir.create("raw", showWarnings = FALSE)
mmr_path <- "raw/KMMRCoverage_County.xlsx"
csv_path <- "raw/KINDERGARTEN_SURVEY.csv"

GRADE <- "Kindergarten"

# The not-fully-immunized categories as the reports label them, and the
# measure stem each becomes (rate_<stem>, N_<stem>).
CATEGORIES <- c(
  "Religious Exemption" = "religious_exempt",
  "Incomplete Record" = "incomplete",
  "Missing Record" = "missing",
  "Temporary Certificate" = "temporary_certificate",
  "Transfer <30 days" = "transfer",
  "Medical Exemption" = "medical_exempt"
)
COUNT_COLS <- c("N_fully_immunized", paste0("N_", CATEGORIES))

# The parsers are defined before the download step because fetch_report()
# runs them on the full download to decide which pages to keep.

# ---- Compliance reports, 2019-20 on ------------------------------------------

# ---- Appendix 1 county tables ----
TABLE1_TITLE <- "Table 1\\. Percentage of (public|public and private) school students"
TABLE2_TITLE <- "Table 2\\. Percentage of private school students"
# The heading that follows the county table. In 2020-21 it shares the
# table's last page.
TABLE_END <- "Summaries by County"

# Rows per year as published. A year not listed here must have between 85
# and 95 counties (TDH has 95; two did not submit in 2023-24 and 2024-25)
# and no county twice.
TABLE1_ROWS <- c("2019" = 95L, "2020" = 95L, "2021" = 95L, "2022" = 95L,
                 "2023" = 93L, "2024" = 93L)
TABLE2_ROWS <- c("2019" = 30L)

# Column order of the three school bands, from the x positions of the
# "<90%" and "95-100%" (or "95-", when the header wraps) cells on the
# table's first page. The color key in the title prints them with
# punctuation attached, so exact matches only see the header.
band_columns <- function(path, page) {
  w <- as.data.frame(pdftools::pdf_data(path)[[page]])
  lo <- w$x[w$text == "<90%"]
  hi <- w$x[w$text %in% c("95-", "95-100%")]
  if (length(lo) != 1L || length(hi) != 1L) {
    stop("TN: expected one '<90%' and one '95-100%' header cell on page ", page,
         " of ", basename(path), ", found ", length(lo), " and ", length(hi),
         call. = FALSE)
  }
  if (hi < lo) c("N_schools_95plus", "N_schools_90_94", "N_schools_under_90")
  else c("N_schools_under_90", "N_schools_90_94", "N_schools_95plus")
}

# County rows of one table: from its title page forward until a page
# yields no rows or carries the next heading. The pages walked, including
# the one the walk stops on, are the "pages" attribute.
county_table <- function(path, text, first, col_names, end) {
  rows <- function(p) {
    pdf_table_rows(path, p, label = "^[A-Z][A-Z .']+$", n_fields = 5L,
                   col_names = col_names, stop = end, text = text)
  }
  out <- list()
  walked <- integer()
  for (p in seq(first, length(text))) {
    walked <- c(walked, p)
    ends_here <- p > first && grepl(end, text[[p]], perl = TRUE)
    r <- rows(p)
    if (nrow(r)) out[[length(out) + 1L]] <- r
    if (ends_here || (p > first && !nrow(r))) break
  }
  structure(bind_rows(out), pages = walked)
}

check_rows <- function(df, expected, year, what) {
  if (!is.na(expected)) {
    pdf_expect_rows(df, expected, sprintf("TN %s %s", what, year))
  } else if (nrow(df) < 85L || nrow(df) > 95L) {
    stop(sprintf("TN %s %s: %d rows is not a county table", what, year, nrow(df)),
         call. = FALSE)
  }
  if (anyDuplicated(df$label)) {
    stop(sprintf("TN %s %s: county listed twice: %s", what, year,
                 paste(unique(df$label[duplicated(df$label)]), collapse = ", ")),
         call. = FALSE)
  }
  invisible(df)
}

# ---- Statewide summaries ----

# Lines from the heading (alone on its line, which the table of contents
# entry is not) to just before `until`, or `n` lines when `until` is NULL.
# The line indices are the "index" attribute.
section_lines <- function(lines, heading, until = NULL, n = 80L) {
  start <- grep(paste0("^\\s*", heading, "\\s*$"), lines, perl = TRUE)
  if (length(start) != 1L) {
    stop("TN: expected one '", heading, "' heading, found ", length(start), call. = FALSE)
  }
  stop_at <- if (is.null(until)) start + n else grep(paste0("^\\s*", until, "\\s*$"), lines, perl = TRUE)
  stop_at <- stop_at[stop_at > start][1]
  if (is.na(stop_at)) stop("TN: no '", until, "' heading after '", heading, "'", call. = FALSE)
  index <- seq(start, min(stop_at - 1L, length(lines)))
  structure(lines[index], index = index)
}

# The first bullet list in a section. pdf_ascii() turns the bullet glyph
# (a different one in 2019-20) into one or more "?"; a blank line or a
# bare page number inside the run (a list that crosses a page break) does
# not end it.
bullet_lines <- function(lines) {
  lines <- pdf_ascii(lines)
  is_bullet <- grepl("^\\s*\\?+\\s+[A-Z]", lines, perl = TRUE)
  first <- which(is_bullet)[1]
  if (is.na(first)) stop("TN: no bullet list in section", call. = FALSE)
  out <- character()
  for (ln in lines[first:length(lines)]) {
    if (grepl("^\\s*\\?+\\s+[A-Z]", ln, perl = TRUE)) {
      out <- c(out, ln)
    } else if (nzchar(trimws(ln)) && !grepl("^\\s*\\d{1,3}\\s*$", ln)) {
      break
    }
  }
  out
}

# "Category: 3.3% (n=2,342)" or "Category: N/A ..." bullets as one row of
# counts and printed shares per category. Every category must be present
# and none may repeat, so a renamed or added category stops the run.
parse_categories <- function(lines, label) {
  m <- str_match(bullet_lines(lines), paste0(
    "^\\s*\\?+\\s*([A-Za-z <>0-9]+?)(?: \\(not shown\\))?:\\s*",
    "(?:([0-9.]+)%\\s*\\(n=\\s*([0-9,]+)\\)|(N/A))"))
  cat_name <- trimws(m[, 2])
  bad <- is.na(cat_name) | !cat_name %in% names(CATEGORIES)
  if (any(bad) || anyDuplicated(cat_name) || !all(names(CATEGORIES) %in% cat_name)) {
    stop("TN ", label, ": category bullets do not match the expected six: ",
         paste(cat_name, collapse = "; "), call. = FALSE)
  }
  stem <- unname(CATEGORIES[cat_name])
  counts <- setNames(as.list(pdf_number(m[, 4])), paste0("N_", stem))
  shares <- setNames(as.list(pdf_number(m[, 3]) / 100), paste0("published_", stem))
  # Every category is a number except the private-school transfer line,
  # which is "N/A".
  unparsed <- is.na(m[, 4]) & is.na(m[, 5])
  if (any(unparsed)) {
    stop("TN ", label, ": category bullet without a count: ",
         paste(cat_name[unparsed], collapse = "; "), call. = FALSE)
  }
  as.data.frame(c(counts, shares))
}

# "92.6% ... (66,574 of 71,927 students)": the fully immunized share and
# the two counts behind it. Earlier percentages in the sentence are ruled
# out because the match cannot cross a parenthesis or another percent.
parse_fully_immunized <- function(lines, label) {
  txt <- gsub("\\s+", " ", paste(pdf_ascii(lines), collapse = " "))
  m <- str_match(txt, "([0-9.]+)%[^()%]{0,200}?\\(([0-9,]+) of ([0-9,]+) students\\)")
  if (is.na(m[1, 1])) stop("TN ", label, ": no 'n of N students' sentence", call. = FALSE)
  data.frame(published_fully_immunized = pdf_number(m[, 2]) / 100,
             N_fully_immunized = pdf_number(m[, 3]),
             N_enrolled = pdf_number(m[, 4]))
}

statewide_row <- function(lines, school_type, label) {
  bind_cols(parse_fully_immunized(lines, label), parse_categories(lines, label)) %>%
    mutate(school_type = school_type)
}

# ---- One report ----
# Returns the county rows, the two statewide rows, and the pages read.
read_report <- function(path) {
  start <- as.integer(str_match(basename(path), "^kindergarten_compliance_(\\d{4})\\.pdf$")[, 2])
  if (is.na(start)) stop("TN: unexpected report file name ", basename(path), call. = FALSE)
  year <- as.character(start)
  text <- pdf_text_pages(path)
  page_lines <- strsplit(text, "\n", fixed = TRUE)
  lines <- unlist(page_lines)
  line_page <- rep(seq_along(text), lengths(page_lines))
  label <- basename(path)

  first <- pdf_find_pages(path, TABLE1_TITLE, text = text)
  if (length(first) != 1L) {
    stop("TN ", label, ": expected one Table 1 title page, found ", length(first), call. = FALSE)
  }
  cols <- c("pct", "n", band_columns(path, first))
  title <- regmatches(text[[first]], regexpr(TABLE1_TITLE, text[[first]], perl = TRUE))
  t1_type <- if (grepl("public and private", title)) "all" else "public"
  t1 <- county_table(path, text, first, cols, paste0(TABLE_END, "|", TABLE2_TITLE))
  pages <- attr(t1, "pages")
  attr(t1, "pages") <- NULL
  t1 <- t1 %>%
    check_rows(TABLE1_ROWS[year], year, "Table 1") %>%
    mutate(school_type = t1_type)

  second <- pdf_find_pages(path, TABLE2_TITLE, text = text)
  t2 <- NULL
  if (length(second) == 1L) {
    t2 <- county_table(path, text, second, cols, TABLE_END)
    pages <- c(pages, attr(t2, "pages"))
    attr(t2, "pages") <- NULL
    t2 <- t2 %>%
      check_rows(TABLE2_ROWS[year], year, "Table 2") %>%
      mutate(school_type = "private")
  } else if (length(second) > 1L) {
    stop("TN ", label, ": more than one Table 2 title page", call. = FALSE)
  }

  counties <- bind_rows(t1, t2) %>%
    transmute(
      time = school_year_time(start), county = label, school_type,
      rate_fully_immunized = parse_rate(pct, from = "percent"),
      N_enrolled = pdf_number(n),
      N_schools_95plus = pdf_number(N_schools_95plus),
      N_schools_90_94 = pdf_number(N_schools_90_94),
      N_schools_under_90 = pdf_number(N_schools_under_90),
      N_schools = N_schools_95plus + N_schools_90_94 + N_schools_under_90
    )
  if (anyNA(counties[-(1:3)])) stop("TN ", label, ": a county cell did not parse", call. = FALSE)

  pub_h <- "Public Schools? Statewide Summary"
  prv_h <- "Private Schools? Statewide Summary"
  pub <- section_lines(lines, pub_h, until = prv_h)
  prv <- section_lines(lines, prv_h)
  pages <- c(pages, line_page[attr(pub, "index")], line_page[attr(prv, "index")])
  state <- bind_rows(
    statewide_row(pub, "public", paste(label, "public")),
    statewide_row(prv, "private", paste(label, "private"))
  ) %>%
    mutate(time = school_year_time(start))

  list(counties = counties, state = state, pages = sort(unique(pages)))
}

# ---- School listings, 2017-18 and 2018-19 ------------------------------------

# First-line header word of each count column and the count it becomes.
SCHOOL_HEADER_COLS <- c(
  Total = "N_enrolled", Fully = "N_fully_immunized", Religious = "N_religious_exempt",
  Medical = "N_medical_exempt", Temporary = "N_temporary_certificate",
  Excluded = "N_transfer", Incomplete = "N_incomplete", Missing = "N_missing"
)

# Counties a listing carries twice under two labels, by "<start> <type>".
# The two blocks are summed. Any other repeat stops the run.
SCHOOL_REPORT_SPLIT_COUNTIES <- list("2018 private" = "Sevier")

# "3,044 (93.3%)" or "3263": the count and the printed share.
school_cell <- function(txt) {
  m <- str_match(txt, "^\\s*([0-9,]+)(?:\\s*\\(([0-9.]+)%\\))?\\s*$")
  data.frame(n = pdf_number(m[, 2]), share = pdf_number(m[, 3]) / 100)
}

# The COUNTY TOTAL and STATEWIDE TOTAL rows of one page with their county
# label and the count and share in each column. NULL for a page without
# the table header (the cover page).
school_page_totals <- function(path, page) {
  w <- pdf_words(path, page)
  hc <- w[w$text == "County", ]
  if (!nrow(hc)) return(NULL)
  # The column header is the leftmost "County"; the page title has one too.
  hc <- hc[which.min(hc$x), ]
  # Count columns: the first header line of the numeric block, just above
  # the "County" line, one word per column. The cover page defines the
  # categories below a "County" line and has no such block.
  top <- w[w$text %in% names(SCHOOL_HEADER_COLS) & w$y < hc$y & w$y > hc$y - 25, ]
  if (!nrow(top)) return(NULL)
  # The county column ends halfway to the next header on the same line
  # (District in the public listing, School in the private one).
  right <- w[abs(w$y - hc$y) <= 2 & w$x > hc$xend, ]
  if (!nrow(right)) stop("TN: no column header right of 'County' on page ", page, call. = FALSE)
  county_bound <- (hc$xend + min(right$x)) / 2
  cols <- sort(setNames(top$xmid, SCHOOL_HEADER_COLS[top$text]))
  if (anyDuplicated(names(cols)) ||
      !all(c("N_enrolled", "N_fully_immunized", "N_missing") %in% names(cols))) {
    stop("TN: count column headers on page ", page, " of ", basename(path), ": ",
         paste(names(cols), collapse = ", "), call. = FALSE)
  }
  half <- min(diff(cols)) / 2
  out <- list()
  prev_y <- hc$y
  for (l in pdf_lines(w)) {
    y0 <- attr(l, "y")
    if (y0 <= hc$y) next
    i <- which(l$text == "TOTAL")
    kind <- l$text[i - 1L]
    kind <- kind[kind %in% c("COUNTY", "STATEWIDE")]
    if (!length(kind)) next
    # Cells within a line of the row, assigned to the nearest column. The
    # statewide fully immunized cell wraps onto the lines above and below.
    cells <- w[abs(w$y - y0) <= 10 & w$xmid > cols[1] - half, ]
    cells$col <- names(cols)[pdf_nearest_col(cells$xmid, cols)]
    cells <- cells[order(cells$col, cells$y, cells$x), ]
    txt <- tapply(cells$text, cells$col, paste, collapse = " ")
    if (!setequal(names(txt), names(cols))) {
      stop("TN: total row at y=", y0, " on page ", page, " of ", basename(path),
           " does not fill every column", call. = FALSE)
    }
    # The county label is centred within its block of rows on the page, so
    # it sits between the previous total row and this one.
    label <- if (kind[1] == "STATEWIDE") "STATEWIDE" else {
      cw <- w[w$xend < county_bound & w$y > prev_y & w$y <= y0 + 4, ]
      paste(cw$text[order(cw$y, cw$x)], collapse = " ")
    }
    row <- data.frame(page = page, kind = kind[1], label = label, stringsAsFactors = FALSE)
    for (cn in names(cols)) {
      v <- school_cell(txt[[cn]])
      row[[cn]] <- v$n
      row[[sub("^N_", "published_", cn)]] <- v$share
    }
    out[[length(out) + 1L]] <- row
    prev_y <- y0
  }
  bind_rows(out)
}

# One listing: county rows (with the canonical county name), the statewide
# row, and the pages read.
read_school_report <- function(path) {
  m <- str_match(basename(path), "^kindergarten_schools_(\\d{4})_(public|private)\\.pdf$")
  if (is.na(m[1, 1])) stop("TN: unexpected school listing file name ", basename(path), call. = FALSE)
  start <- as.integer(m[, 2])
  school_type <- m[, 3]
  label <- basename(path)
  text <- pdf_text_pages(path)
  # The page titles state the school type and year; the file name must agree.
  title <- str_match(text, "(Public|Private) Schools by County(?: and District)?, School Year (20\\d{2})-20\\d{2}")
  stated <- unique(paste(tolower(title[, 2]), title[, 3])[!is.na(title[, 1])])
  if (!identical(stated, paste(school_type, start))) {
    stop("TN ", label, ": the page titles say '", paste(stated, collapse = "', '"), "'", call. = FALSE)
  }

  totals <- bind_rows(lapply(seq_along(text), function(p) school_page_totals(path, p)))
  counts <- grep("^N_", names(totals), value = TRUE)
  if (!nrow(totals) || anyNA(totals[counts])) {
    stop("TN ", label, ": a total cell did not parse", call. = FALSE)
  }
  state <- totals[totals$kind == "STATEWIDE", ]
  counties <- totals[totals$kind == "COUNTY", ]
  if (nrow(state) != 1L) stop("TN ", label, ": expected one STATEWIDE TOTAL row, found ", nrow(state), call. = FALSE)
  if (nrow(counties) < 20L || nrow(counties) > 95L) {
    stop("TN ", label, ": ", nrow(counties), " COUNTY TOTAL rows", call. = FALSE)
  }
  # The printed shares are rounded to 0.1 percent, so they only police the
  # counts, and the statewide row must be the sum of the county rows.
  for (cn in setdiff(counts, "N_enrolled")) {
    check_rate_against_counts(totals[[sub("^N_", "published_", cn)]], totals[[cn]],
                              totals$N_enrolled, label = paste("TN", label, cn), tol = 0.002)
  }
  if (!all(colSums(counties[counts]) == unlist(state[counts]))) {
    stop("TN ", label, ": county totals do not sum to the statewide row", call. = FALSE)
  }

  counties$county <- canonical_county_name(counties$label, "TN")
  if (anyNA(counties$county)) {
    stop("TN ", label, ": not a county: ",
         paste(unique(counties$label[is.na(counties$county)]), collapse = "; "), call. = FALSE)
  }
  dup <- unique(counties$county[duplicated(counties$county)])
  allowed <- SCHOOL_REPORT_SPLIT_COUNTIES[[paste(start, school_type)]]
  if (!setequal(dup, if (is.null(allowed)) character() else allowed)) {
    stop("TN ", label, ": county listed twice: ", paste(dup, collapse = ", "), call. = FALSE)
  }
  counties <- counties %>%
    group_by(county) %>%
    summarise(across(all_of(counts), sum), .groups = "drop")
  for (cn in setdiff(counts, "N_enrolled")) {
    counties[[sub("^N_", "rate_", cn)]] <- rate_from_counts(counties[[cn]], counties$N_enrolled)
  }
  counties <- counties %>%
    mutate(time = school_year_time(start), school_type = school_type)
  state <- state %>%
    select(-page, -kind, -label) %>%
    mutate(time = school_year_time(start), school_type = school_type)
  list(counties = counties, state = state, pages = sort(unique(totals$page)))
}

# ---- Download ----------------------------------------------------------------

# The MMR workbook is a one-off supplied file, not something TDH republishes,
# so it is only fetched when the committed copy is missing or does not open.
process <- record_fetch(process, fetch_file(
  mmr_src$url, mmr_path, type = "xlsx", if_exists = "skip",
  previous = prev[[mmr_path]]
))

# School-year start from a compliance report file name. TDH has used
# "K-Survey-Report-2021-2022.pdf", "K-Survey-2023-2024.pdf",
# "2024-2025-Tennessee-Kindergarten-...-Report.pdf" and, for 2019-20,
# "2020_KindergartenSurveyReport.pdf" (named by the school-year end).
report_start_year <- function(url) {
  fn <- basename(url)
  m <- str_match(fn, "(20\\d{2})-20\\d{2}")[, 2]
  if (!is.na(m)) return(as.integer(m))
  m <- str_match(fn, "^(20\\d{2})_KindergartenSurveyReport")[, 2]
  if (!is.na(m)) return(as.integer(m) - 1L)
  stop("TN: no school year in report file name ", fn, call. = FALSE)
}

# raw/ path of a school listing. "2018_Public_Report_FINAL.PDF" is 2017-18
# (named by the school-year end); "2018_19_Public_Report.pdf" is 2018-19.
school_report_dest <- function(url) {
  m <- str_match(basename(url), "^(20\\d{2})(?:_(\\d{2}))?_(Public|Private)_Report")
  if (is.na(m[1, 1])) stop("TN: unexpected school listing file name ", basename(url), call. = FALSE)
  start <- as.integer(m[, 2]) - if (is.na(m[, 3])) 1L else 0L
  sprintf("raw/kindergarten_schools_%d_%s.pdf", start, tolower(m[, 4]))
}

FETCH_RETRIES <- 3L

# Download a report to a temporary file, parse it with `reader`, and write
# the pages the parser read to `dest` with qpdf::pdf_subset(). `reader`
# returns a list with a `pages` element and stops when the file does not
# parse. The subset must parse to the same result as the full file.
#
# Nothing is downloaded when `dest` exists and parses. A download or parse
# failure keeps the existing subset with a warning and a "failed" record;
# with no subset to fall back on it stops, like fetch_file(). The record
# describes the subset, not the download.
fetch_report <- function(url, dest, reader, label = basename(dest)) {
  have <- file.exists(dest)
  if (have) {
    ok <- tryCatch({ reader(dest); TRUE }, error = function(e) {
      warning("TN: ", label, " in raw/ does not parse (", conditionMessage(e),
              "); re-downloading", call. = FALSE)
      FALSE
    })
    if (ok) {
      return(fetch_record(url, dest, "skipped", bytes = file.size(dest),
                          sha256 = fetch_sha256(dest)))
    }
  }
  give_up <- function(msg) {
    if (!have) {
      stop("fetch: ", label, " could not be fetched from ", url, " (", msg,
           ") and there is no committed copy to fall back on", call. = FALSE)
    }
    warning("fetch: ", label, " could not be refreshed from ", url, " (", msg,
            "); keeping the committed copy", call. = FALSE)
    fetch_record(url, dest, "failed", bytes = file.size(dest), sha256 = fetch_sha256(dest),
                 attempts = FETCH_RETRIES, error = msg)
  }
  # The parsers take the school year from the file name, so the download
  # is named like dest.
  tmp_dir <- tempfile("tn_report_")
  dir.create(tmp_dir)
  on.exit(unlink(tmp_dir, recursive = TRUE), add = TRUE)
  tmp <- file.path(tmp_dir, basename(dest))
  rec <- tryCatch(
    fetch_file(url, tmp, type = "pdf", if_exists = "replace", conditional = FALSE,
               timeout = 600, retries = FETCH_RETRIES, label = label),
    error = function(e) e
  )
  if (inherits(rec, "error")) return(give_up(conditionMessage(rec)))
  full <- tryCatch(reader(tmp), error = function(e) e)
  if (inherits(full, "error")) {
    return(give_up(paste("the downloaded report does not parse:", conditionMessage(full))))
  }
  sub_dir <- file.path(tmp_dir, "subset")
  dir.create(sub_dir)
  subset <- file.path(sub_dir, basename(dest))
  qpdf::pdf_subset(tmp, pages = full$pages, output = subset)
  again <- reader(subset)
  if (!identical(again[names(again) != "pages"], full[names(full) != "pages"])) {
    stop("TN: the page subset of ", label, " parses differently from the full report",
         call. = FALSE)
  }
  new_sha <- fetch_sha256(subset)
  if (have && identical(new_sha, fetch_sha256(dest))) {
    rec$status <- "unchanged"
  } else {
    if (!file.copy(subset, dest, overwrite = TRUE)) stop("TN: could not write ", dest, call. = FALSE)
    message(sprintf("fetch: kept %d of %d pages of %s (%s bytes)", length(full$pages),
                    pdftools::pdf_info(tmp)$pages, label, format(file.size(dest), big.mark = ",")))
  }
  rec$dest <- dest
  rec$bytes <- file.size(dest)
  rec$sha256 <- new_sha
  rec
}

# A report never changes once posted, so a subset already in raw/ that
# parses is not re-requested. The index page being unreachable (tn.gov
# resets connections and 404s intermittently) is a warning; the committed
# files are parsed either way.
links <- discover_links(pdf_src$page_url,
                        paste0("(?:", pdf_src$pattern, ")|(?:", sch_src$pattern, ")"),
                        must_find = FALSE)
if (nrow(links)) {
  is_school <- grepl(sch_src$pattern, links$url, ignore.case = TRUE, perl = TRUE)
  dests <- character(nrow(links))
  dests[is_school] <- vapply(links$url[is_school], school_report_dest, character(1), USE.NAMES = FALSE)
  dests[!is_school] <- sprintf("raw/kindergarten_compliance_%d.pdf",
                               vapply(links$url[!is_school], report_start_year, integer(1), USE.NAMES = FALSE))
  recs <- vector("list", nrow(links))
  for (i in seq_len(nrow(links))) {
    recs[[i]] <- fetch_report(links$url[i], dests[i],
                              if (is_school[i]) read_school_report else read_report)
    if (i < nrow(links) && recs[[i]]$status != "skipped") Sys.sleep(3)
  }
  fetch_summary(recs, "TN reports")
  process <- record_fetch(process, recs)
}

# The survey CSV is overwritten in place when TDH adds a year.
process <- record_fetch(process, fetch_file(
  csv_src$url, csv_path, type = "csv",
  expect_cols = c("school_type", "county", "total_enrolled", "complete_record"),
  previous = prev[[csv_path]]
))

# Written now as well as at the end, so a parse that stops on a raw file a
# failed fetch could not refresh still leaves the "failed" record behind.
commit_fetch_state(process)

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # ---- Reports ----------------------------------------------------------------
  report_files <- list.files("raw", pattern = "^kindergarten_compliance_\\d{4}\\.pdf$",
                             full.names = TRUE)
  if (!length(report_files)) stop("TN: no compliance reports in raw/", call. = FALSE)
  school_files <- list.files("raw", pattern = "^kindergarten_schools_\\d{4}_(public|private)\\.pdf$",
                             full.names = TRUE)
  reports <- c(lapply(report_files, read_report), lapply(school_files, read_school_report))
  pdf_counties <- bind_rows(lapply(reports, `[[`, "counties"))
  pdf_state <- bind_rows(lapply(reports, `[[`, "state"))

  # Known error in the source: the 2022-23 private school summary prints
  # "Missing Record: 1.1% (n=107)". 107 is the 2021-22 count (2.5% of 4,290
  # students) and 1.1% of the 5,097 students in 2022-23 is 56; the arrow
  # beside it (down 1.4 points) agrees with the share, not the count. The
  # count is dropped and the printed share is kept for that cell.
  pdf_state$N_missing[pdf_state$time == "2022-09-01" & pdf_state$school_type == "private"] <- NA_real_

  # The printed shares are rounded to 0.1 percent (2023-24 prints medical
  # exemptions as 0.0% with n=94), so they only police the counts.
  for (stem in c("fully_immunized", CATEGORIES)) {
    check_rate_against_counts(
      pdf_state[[paste0("published_", stem)]], pdf_state[[paste0("N_", stem)]],
      pdf_state$N_enrolled, label = paste("TN statewide", stem), tol = 0.002)
  }

  # Private schools have no transfer figure ("N/A" in the summaries, no
  # column in the listings), so the combined row has none either. Every
  # other count is the sum of the two school types, and a sum with a
  # dropped count is itself dropped.
  state_all <- pdf_state %>%
    group_by(time) %>%
    summarise(across(all_of(c("N_enrolled", setdiff(COUNT_COLS, "N_transfer"))), sum),
              school_type = "all", .groups = "drop")
  state_rows <- bind_rows(pdf_state, state_all)
  for (stem in c("fully_immunized", CATEGORIES)) {
    published <- state_rows[[paste0("published_", stem)]]
    if (is.null(published)) published <- NA_real_
    state_rows[[paste0("rate_", stem)]] <- coalesce(
      rate_from_counts(state_rows[[paste0("N_", stem)]], state_rows$N_enrolled), published)
  }
  state_rows <- state_rows %>%
    select(-starts_with("published_")) %>%
    mutate(geography = "47", geography_name = "Tennessee", type = "state", grade = GRADE)

  # ---- School-level survey file -----------------------------------------------
  schools <- vroom(csv_path, col_types = cols(.default = col_character()),
                   show_col_types = FALSE, progress = FALSE)
  # The first header cell carries a byte-order mark in the posted file.
  names(schools)[1] <- sub("^\\W+", "", names(schools)[1])
  csv_cols <- c(
    School_Year = "start", school_type = "school_type", county = "county",
    SCHOOL_NAME = "school_name", total_enrolled = "N_enrolled",
    complete_record = "N_fully_immunized", missing_record = "N_missing",
    exempt_religious = "N_religious_exempt", temp_certificate = "N_temporary_certificate",
    exempt_medical = "N_medical_exempt", incomplete_record = "N_incomplete"
  )
  if (!setequal(names(schools), names(csv_cols))) {
    stop("TN: KINDERGARTEN_SURVEY.csv columns changed: ", paste(names(schools), collapse = ", "),
         call. = FALSE)
  }
  schools <- schools[names(csv_cols)]
  names(schools) <- unname(csv_cols)
  schools <- schools %>%
    mutate(
      start = as.integer(start),
      school_type = tolower(trimws(school_type)),
      county = trimws(county),
      school_name = trimws(school_name),
      across(starts_with("N_"), clean_numeric)
    )
  if (anyNA(schools) || !all(schools$school_type %in% c("public", "private"))) {
    stop("TN: KINDERGARTEN_SURVEY.csv has a blank cell or an unexpected school type", call. = FALSE)
  }
  csv_counts <- setdiff(COUNT_COLS, "N_transfer")

  school_rows <- schools %>%
    mutate(time = school_year_time(start), type = "school", grade = GRADE) %>%
    join_county_fips("TN")
  for (col in csv_counts) {
    school_rows[[sub("^N_", "rate_", col)]] <- rate_from_counts(school_rows[[col]], school_rows$N_enrolled)
  }
  school_rows <- school_rows %>%
    select(time, geography, geography_name, type, school_type, school_name, grade,
           N_enrolled, all_of(csv_counts), starts_with("rate_")) %>%
    arrange(time, geography, school_type, school_name)

  # County sums of the school rows, by school type and over both types. These
  # are kept apart from the report's county table: the two agree on the
  # statewide totals, but the 2019-20 survey file places schools in
  # different counties from the 2019-20 report for 38 of the 95 counties
  # (Anderson 763 students against 853 in the report), and Carter County
  # differs in 2020-21 and 2021-22. Which of the two is the county of the
  # school is not stated, so neither is used to fill the other.
  sum_counties <- function(d) {
    d %>%
      group_by(time = school_year_time(start), county, school_type) %>%
      summarise(N_schools = n(), across(all_of(c("N_enrolled", csv_counts)), sum),
                .groups = "drop")
  }
  survey_counties <- bind_rows(
    sum_counties(schools),
    sum_counties(mutate(schools, school_type = "all"))
  ) %>%
    join_county_fips("TN") %>%
    mutate(type = "county", grade = GRADE)
  for (col in csv_counts) {
    survey_counties[[sub("^N_", "rate_", col)]] <-
      rate_from_counts(survey_counties[[col]], survey_counties$N_enrolled)
  }
  survey_counties <- survey_counties %>%
    select(time, geography, geography_name, type, school_type, grade,
           N_schools, N_enrolled, all_of(csv_counts), starts_with("rate_")) %>%
    arrange(time, geography, school_type)

  # ---- MMR workbook (2024-25) -------------------------------------------------
  # Rounded to one decimal on the percent scale the workbook publishes before
  # the conversion to a rate. The county survey covers public and private
  # schools, so the figure sits on the combined-school county row.
  mmr <- readxl::read_excel(mmr_path, sheet = "Data") %>%
    transmute(
      time = school_year_time(2024L),
      county = trimws(as.character(county)),
      school_type = "all",
      rate_mmr = parse_rate(round(suppressWarnings(as.numeric(percent_mmr)), 1), from = "percent")
    ) %>%
    filter(!is.na(county), county != "") %>%
    join_county_fips("TN") %>%
    select(-county)

  # The report prints county names in capitals and the workbook in title
  # case, so the two are joined on the FIPS code each resolves to.
  county_rows <- pdf_counties %>%
    join_county_fips("TN") %>%
    select(-county) %>%
    full_join(mmr, by = c("time", "geography", "geography_name", "school_type")) %>%
    mutate(type = "county", grade = GRADE)

  # ---- Assemble ---------------------------------------------------------------
  out <- bind_rows(county_rows, state_rows) %>%
    select(time, geography, geography_name, type, school_type, grade,
           rate_fully_immunized, rate_mmr, N_enrolled, N_fully_immunized,
           N_schools, N_schools_95plus, N_schools_90_94, N_schools_under_90,
           any_of(as.vector(rbind(paste0("rate_", CATEGORIES), paste0("N_", CATEGORIES))))) %>%
    arrange(time, type, geography, school_type)

  if (anyNA(out$geography) || anyNA(survey_counties$geography) || anyNA(school_rows$geography)) {
    stop("TN: a row has no geography", call. = FALSE)
  }

  message(sprintf(
    paste0("TN: %d county rows and %d state rows from the reports, %d county rows and ",
           "%d school rows from the survey file; school years %s to %s"),
    sum(out$type == "county"), sum(out$type == "state"), nrow(survey_counties),
    nrow(school_rows), min(out$time), max(out$time)))

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(out, "Tennessee", "standard/data.csv.gz", from = "rate")
  write_standard(survey_counties, "Tennessee survey counties",
                 "standard/data_survey_counties.csv.gz", from = "rate")
  write_standard(school_rows, "Tennessee schools", "standard/data_schools.csv.gz", from = "rate")
  update_latest_year(latest_school_year(out),
                     ids = c("tdh_mmr_county", "kindergarten_compliance_pdf"))
  if (length(school_files)) {
    update_latest_year(latest_school_year(bind_rows(lapply(reports[-seq_along(report_files)], `[[`, "counties"))),
                       ids = "kindergarten_school_reports_pdf")
  }
  update_latest_year(latest_school_year(school_rows), ids = "kindergarten_survey_csv")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
