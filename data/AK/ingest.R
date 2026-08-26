library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(tidyr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")

# =============================================================================
# AK - Kindergarten (5-6 years) and adolescent (13-17 years) vaccination
#   coverage, by DPH region
# Source: Alaska DHSS, "Alaska Vaccination Coverage Report" (quarterly),
#   built from VacTrAK (the state immunization registry), not from a school
#   entry survey. Each workbook is a formatted report with several tables;
#   two are school-relevant and ingested here: "Table 2: ... Kindergarten
#   Series" and "Table 3: ... [Adolescent Series /] 13-17 years" (its 19-35
#   month and adult tables are not). Each gives one coverage percent per
#   antigen series for each of Alaska's 7 public-health regions (Anchorage,
#   Gulf Coast, Interior, Mat-Su, Northern, Southeast, Southwest) plus a
#   statewide figure.
#
# Some quarters drop the adolescent Tdap row (and, with it, the overall
# up-to-date row, which cannot be computed without it) -- VacTrAK had a
# forecasting error that made Tdap unassessable for adolescents and adults
# for a time -- so Table 3 does not always carry the same set of rows. Rows
# are matched by label rather than by position or count for this reason.
#
# Most regions bundle several boroughs/census areas and have no single FIPS
# of their own (Gulf Coast, Interior, Mat-Su, Northern, Southeast,
# Southwest); they are kept with geography = NA (join_county_fips()'s
# `no_fips`) rather than guessed onto one borough. Anchorage is the one
# exception: it is coextensive with the Municipality of Anchorage, a single
# home-rule borough, so it resolves to that borough's real FIPS through the
# ordinary county join instead of being forced into `no_fips`.
#
# No exemption or enrollment counts are published in either table -- both
# are a per-series coverage percent only -- so no N_* columns are produced.
# =============================================================================

AK_REGIONS <- c("Anchorage", "Gulf Coast", "Interior", "Mat-Su", "Northern",
                 "Southeast", "Southwest")

# The source abbreviates Anchorage as "Anc"; every other region label is used
# as printed.
REGION_LABEL_MAP <- c(Anc = "Anchorage")

# One block per school-relevant table: which "Table N:" to look for, the
# `grade` stratum its rows belong to, and its row label -> canonical measure
# key mapping. Labels are matched by substring rather than exact text, since
# the source's own labels carry the dose count ("5 DTaP/DT/Td UTD", "3 Hep
# B") which is not part of the identity of the series.
TABLE_SPECS <- list(
  list(number = 2, grade = "Kindergarten", patterns = c(
    "^Kindergarten Series" = "complete",
    "DTaP" = "dtap",
    "Polio" = "polio",
    "Hep[ -]?B" = "hep_b",
    "MMR" = "mmr",
    "Varicella" = "varicella",
    "Hep[ -]?A" = "hep_a"
  )),
  list(number = 3, grade = "Adolescent", patterns = c(
    "^Adolescent Series" = "complete",
    "Tdap" = "tdap",
    "HPV" = "hpv",
    "MenACWY" = "menacwy"
  ))
)

squish_label <- function(x) str_squish(gsub("[\r\n]+", " ", x))

# First non-NA, non-blank cell of each element in a character vector, in
# column order -- used on both the header row (labels only) and each data row
# (a leading measure label followed by one value per header label). Kept as a
# plain positional match rather than matching header text to data columns
# directly, because the workbook's regions sit behind a variable number of
# blank (merged-cell) columns that differs by year -- but the COUNT and ORDER
# of non-blank cells in a data row always lines up with the header's, which is
# what several years of this report were checked against.
non_blank <- function(x) {
  x <- trimws(as.character(x))
  x[!is.na(x) & x != ""]
}

match_measure <- function(label, patterns, path) {
  hits <- names(patterns)[vapply(names(patterns), function(p) {
    grepl(p, label, ignore.case = TRUE)
  }, logical(1))]
  if (length(hits) != 1) {
    stop("AK: row label '", label, "' in ", path, " matched ", length(hits),
         " known vaccine pattern(s) (expected 1) -- check TABLE_SPECS.",
         call. = FALSE)
  }
  patterns[[hits]]
}

# Parse one "Table N:" block of a workbook into a long data frame of
# region/measure/value. Shares report_year/report_quarter/time/first_col/
# table_rows with the caller, which are the same for every table in the file.
parse_table_block <- function(raw, first_col, table_rows, spec,
                               report_year, report_quarter, path) {
  title_row <- which(str_detect(first_col, paste0("^Table\\s+", spec$number, ":")))
  if (length(title_row) != 1) {
    stop("AK: expected exactly one 'Table ", spec$number, ":' row in ", path,
         ", found ", length(title_row), call. = FALSE)
  }
  later <- table_rows[table_rows > title_row]
  block_end <- if (length(later)) min(later) - 1L else nrow(raw)

  header_row <- title_row + 1L
  header_labels <- squish_label(non_blank(raw[header_row, ]))

  alaska_candidates <- header_labels[str_detect(header_labels, "^Alaska")]
  alaska_match <- alaska_candidates[
    str_detect(alaska_candidates, as.character(report_year)) &
      str_detect(alaska_candidates, paste0("Q", report_quarter))
  ]
  if (length(alaska_match) != 1) {
    stop(sprintf(
      "AK: expected exactly one statewide column matching %d Q%d in Table %s of %s, found %d (%s).",
      report_year, report_quarter, spec$number, path, length(alaska_match),
      paste(alaska_candidates, collapse = "; ")), call. = FALSE)
  }

  region_labels <- unname(ifelse(header_labels %in% names(REGION_LABEL_MAP),
                                  REGION_LABEL_MAP[header_labels], header_labels))
  keep <- header_labels == alaska_match | region_labels %in% AK_REGIONS
  if (sum(keep) != length(AK_REGIONS) + 1) {
    stop("AK: expected ", length(AK_REGIONS), " region columns plus 1 statewide ",
         "column in Table ", spec$number, " of ", path, ", matched ", sum(keep),
         " of ", length(header_labels), " header label(s): ",
         paste(header_labels, collapse = "; "), call. = FALSE)
  }
  col_labels <- ifelse(header_labels == alaska_match, "Alaska", region_labels)[keep]

  rows <- lapply((header_row + 1L):block_end, function(r) {
    cells <- non_blank(raw[r, ])
    if (!length(cells)) return(NULL)
    label <- cells[[1]]
    values <- cells[-1]
    if (length(values) != length(header_labels)) {
      stop("AK: row '", label, "' in Table ", spec$number, " of ", path, " has ",
           length(values), " value(s), expected ", length(header_labels),
           " (one per header column) -- layout may have changed.", call. = FALSE)
    }
    tibble(
      county = col_labels,
      measure = match_measure(label, spec$patterns, path),
      value = readr::parse_number(values[keep])
    )
  })

  bind_rows(rows) %>%
    mutate(grade = spec$grade)
}

# Parse every school-relevant table (TABLE_SPECS) out of one workbook, dated
# to the school year the report's own quarter falls in.
parse_workbook <- function(path) {
  raw <- readxl::read_excel(path, sheet = 1, col_names = FALSE, col_types = "text")
  first_col <- trimws(as.character(raw[[1]]))

  title_cell <- first_col[[1]]
  m <- str_match(title_cell, "Quarter\\s*(\\d)\\D+(\\d{4})")
  if (any(is.na(m[1, ]))) {
    stop("AK: could not find 'Quarter N, YYYY' in the title cell of ", path,
         call. = FALSE)
  }
  report_quarter <- as.integer(m[1, 2])
  report_year <- as.integer(m[1, 3])

  # The report is a snapshot as of the LAST day of that quarter. Q1 (through
  # Mar 31) and Q2 (through Jun 30) fall within the school year that ENDED
  # that calendar year; Q3 (through Sep 30) and Q4 (through Dec 31) fall
  # within the school year that STARTED that calendar year.
  school_year_start <- if (report_quarter <= 2) report_year - 1L else report_year
  time <- as.Date(school_year_time(school_year_start))

  table_rows <- which(str_detect(first_col, "^Table\\s+\\d+:"))

  blocks <- lapply(TABLE_SPECS, function(spec) {
    parse_table_block(raw, first_col, table_rows, spec, report_year,
                       report_quarter, path)
  })

  bind_rows(blocks) %>%
    mutate(time = time, file = basename(path))
}

# Excludes Excel's own "~$..." lock files, which briefly appear alongside a
# workbook while it is open in Excel and are not part of the actual source
# data.
drop_lock_files <- function(paths) paths[!grepl("^~\\$", basename(paths))]

raw_state <- as.list(tools::md5sum(drop_lock_files(list.files(
  "raw", recursive = TRUE, full.names = TRUE
))))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_files <- drop_lock_files(list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE))

  long_data <- bind_rows(lapply(raw_files, parse_workbook))

  data_out <- long_data %>%
    mutate(measure = paste0("pct_", measure)) %>%
    pivot_wider(names_from = measure, values_from = value) %>%
    select(-file) %>%
    join_county_fips("AK", statewide = "Alaska",
                      no_fips = setdiff(AK_REGIONS, "Anchorage")) %>%
    mutate(
      type = case_when(
        geography_name == "Alaska" ~ "state",
        !is.na(geography) ~ "county",
        TRUE ~ "region"
      )
    ) %>%
    select(-county) %>%
    arrange(time, grade, geography_name)

  message(sprintf(
    "AK: %d rows from %d workbook(s), %s",
    nrow(data_out), length(raw_files),
    paste(sort(unique(data_out$time)), collapse = ", ")))

  write_standard(data_out, "Alaska", "./standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
