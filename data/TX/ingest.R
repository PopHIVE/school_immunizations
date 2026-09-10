library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")

# =============================================================================
# TX - School vaccine coverage and conscientious exemptions by county
# Source: Texas DSHS, Annual Report of Immunization Status of Students.
#
# Two index pages, both scraped so a new school year arrives on its own:
#
#   coverage   https://www.dshs.texas.gov/immunizations/data/school/coverage
#     One workbook per school year and grade (Kindergarten, Seventh Grade),
#     2019-20 onward. Sheet "Coverage by County" is one row per county plus a
#     "Texas" total; sheet "Coverage by District" is one row per public ISD or
#     private school (schools with 5 or fewer students in the grade are left
#     out by DSHS). Both give a proportion per required antigen. "NR" / "NR**"
#     means no report: the county's few schools did not answer the survey or
#     had no class in that grade.
#   exemptions https://www.dshs.texas.gov/immunizations/data/school/conscientious-exemptions
#     A multi-year "Conscientious Exemptions by County" workbook (sheets
#     Kindergarten, 7th Grade, K-12; one column per school year), reissued
#     each year with the window shifted. Three overlapping issues are posted
#     (2013-2024, 2015-2025, 2016-2026); they are unioned and, where a
#     county-year appears in more than one, the newest issue wins (the issues
#     agree wherever they overlap).
#
#     This series used to come from three hand-built CSVs (TX_kg_2014-24.csv,
#     TX_7th_2014-24.csv, TX_K-12_2014-24.csv). Those had been assembled with
#     the county names re-sorted -- McCulloch, McLennan and McMullen ahead of
#     Madison, El Paso ahead of Ellis -- while the values kept the workbook's
#     order, so eleven counties (Ellis, El Paso, Madison, Marion, Martin,
#     Mason, Matagorda, Maverick, McCulloch, McLennan, McMullen) carried a
#     neighbour's rate in every year and grade. Every other cell agreed with
#     the workbook to the two decimals the CSVs had rounded to. The CSVs were
#     removed and the workbooks are read directly.
#
# Every value in every workbook is a proportion (Excel-formatted as a percent),
# so both sources are written with from = "rate".
#
# Three of the coverage files are posted with an .xls name but are xlsx inside
# (2019-20 Seventh Grade, both 2021-22 files). readxl chooses its reader by
# extension, so read_excel() and excel_sheets() fail on them with a libxls
# error. The files are kept under their posted names and opened through
# workbook_path(), which sniffs the signature and hands readxl a copy with the
# right extension. The same applies to the download validator, which is why
# fetch_many() is called with type = "any" and a validator of its own rather
# than type = "xls".
# =============================================================================

sources <- read_sources()
cov_src <- source_entry(sources, "dshs_coverage_workbooks")
ex_src <- source_entry(sources, "dshs_conscientious_exemptions")

process <- dcf::dcf_process_record()
prev <- process$fetch_state

# "xlsx" or "xls" from the file signature, NA for anything else (an HTML
# block page, a truncated transfer).
excel_signature <- function(path) {
  sig <- readBin(path, "raw", n = 4L)
  if (identical(sig, as.raw(c(0x50, 0x4b, 0x03, 0x04)))) return("xlsx")
  if (identical(sig, as.raw(c(0xd0, 0xcf, 0x11, 0xe0)))) return("xls")
  NA_character_
}

# A path readxl can open: the file itself when its extension matches its
# signature, otherwise a temporary copy under the right extension.
workbook_path <- function(path) {
  fmt <- excel_signature(path)
  if (is.na(fmt)) stop("TX: ", basename(path), " is not an Excel workbook", call. = FALSE)
  if (identical(tolower(tools::file_ext(path)), fmt)) return(path)
  tmp <- tempfile(fileext = paste0(".", fmt))
  file.copy(path, tmp, overwrite = TRUE)
  tmp
}

check_workbook <- function(path) {
  sh <- readxl::excel_sheets(workbook_path(path))
  if (!length(sh)) stop("workbook has no sheets")
}

# ---- Download ----------------------------------------------------------------
# Per-year coverage workbooks never change once posted, and each exemption
# issue is a fixed file, so anything already on disk that opens is not
# re-requested. The index pages being unreachable must not stop the run:
# raw/ is committed, so discovery warns and the parse proceeds on what is
# there.
fetch_index <- function(src, subdir) {
  links <- discover_links(src$page_url, src$pattern, must_find = FALSE)
  if (!nrow(links)) return(list())
  recs <- fetch_many(
    links$url,
    dest_fn = function(u) file.path("raw", subdir, basename(u)),
    type = "any", validate = check_workbook, if_exists = "skip",
    previous = prev
  )
  fetch_summary(recs, paste("TX", subdir))
  recs
}

process <- record_fetch(process, fetch_index(cov_src, "coverage"))
process <- record_fetch(process, fetch_index(ex_src, "exemptions"))

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # Every cell is read as text so a numeric proportion and an "NR**" marker
  # can share a column; clean_numeric() turns the marker into NA and
  # censor_flag() records it as "missing". The asterisks are a footnote
  # reference, not part of the marker, and censor_flag() knows "NR" but not
  # "NR**", so they are stripped first.
  no_report <- function(x) sub("^\\s*(nr)\\**\\s*$", "\\1", x, ignore.case = TRUE)

  read_sheet <- function(path, sheet) {
    suppressMessages(readxl::read_xlsx(
      workbook_path(path), sheet = sheet, col_names = FALSE, col_types = "text"
    ))
  }

  sheet_named <- function(path, pattern) {
    sh <- readxl::excel_sheets(workbook_path(path))
    hit <- grep(pattern, sh, ignore.case = TRUE, value = TRUE)
    if (length(hit) != 1L) {
      stop("TX: expected one sheet matching /", pattern, "/ in ", basename(path),
           ", found: ", paste(sh, collapse = ", "), call. = FALSE)
    }
    hit
  }

  # The header is the first row with a "County" cell; the title and note rows
  # above it vary in number (two in most files, none in 2023-24 Kindergarten).
  header_row <- function(d) {
    hit <- which(apply(d, 1, function(r) any(tolower(trimws(r)) == "county", na.rm = TRUE)))
    if (!length(hit)) stop("TX: no header row with a County column", call. = FALSE)
    hit[1]
  }

  # Antigen headers as printed by DSHS. "DTP/DTaP/DT/Td" is the kindergarten
  # series and "Tdap/Td" the seventh-grade booster, so the DTP test has to run
  # first or "Td" would catch both. Anything else in the header must be one of
  # the identifying columns; an unknown header stops the run so a new antigen
  # cannot be dropped in silence.
  antigen_measure <- function(h) {
    x <- tolower(trimws(h))
    dplyr::case_when(
      grepl("^dtp|dtap", x) ~ "pct_dtap",
      grepl("^tdap", x) ~ "pct_tdap",
      grepl("mening", x) ~ "pct_menacwy",
      grepl("hepatitis a|hep a", x) ~ "pct_hep_a",
      grepl("hepatitis b|hep b", x) ~ "pct_hep_b",
      grepl("mmr", x) ~ "pct_mmr",
      grepl("polio", x) ~ "pct_polio",
      grepl("varicella", x) ~ "pct_varicella",
      TRUE ~ NA_character_
    )
  }
  id_columns <- c(
    "county" = "county", "facility number" = "facility_id",
    "school type" = "school_type", "facility name" = "district",
    "facility address" = "address"
  )

  # One sheet of a coverage workbook as a frame with named columns.
  read_coverage_sheet <- function(path, sheet) {
    d <- read_sheet(path, sheet)
    hdr <- header_row(d)
    header <- trimws(as.character(unlist(d[hdr, ])))
    role <- antigen_measure(header)
    is_id <- tolower(header) %in% names(id_columns)
    role[is_id] <- unname(id_columns[tolower(header[is_id])])
    unknown <- header[is.na(role) & !is.na(header) & header != ""]
    if (length(unknown)) {
      stop("TX: unrecognised column(s) in ", basename(path), " / ", sheet, ": ",
           paste(unknown, collapse = ", "), call. = FALSE)
    }
    body <- d[seq_len(nrow(d)) > hdr, !is.na(role), drop = FALSE]
    names(body) <- role[!is.na(role)]
    body %>%
      mutate(county = trimws(county)) %>%
      filter(!is.na(county), county != "")
  }

  # School year and grade from the posted file name. DSHS has used
  # "2019-2020-...-Kindergarten", "22-23-...-K", "2025-2026-...-kg" and
  # "..._Seventh_Grade"; a name that fits none of them stops the run.
  coverage_meta <- function(path) {
    fn <- basename(path)
    m <- str_match(fn, "^(20\\d{2}|\\d{2})[-_](?:20\\d{2}|\\d{2})")
    start <- m[, 2]
    if (is.na(start)) stop("TX: no school year in file name ", fn, call. = FALSE)
    if (nchar(start) == 2L) start <- paste0("20", start)
    grade <- if (grepl("(^|[-_])(k|kg|kindergarten)([-_.]|$)", fn, ignore.case = TRUE)) {
      "Kindergarten"
    } else if (grepl("seventh|7th", fn, ignore.case = TRUE)) {
      "7th grade"
    } else {
      stop("TX: no grade in file name ", fn, call. = FALSE)
    }
    list(start = as.integer(start), grade = grade)
  }

  # DSHS labels public districts "Public ISD" in some years and "Public
  # School" in others; both are the same thing, so the label is reduced to
  # Public / Private. A third label would stop the run.
  school_type_label <- function(x) {
    x <- trimws(x)
    out <- dplyr::case_when(
      grepl("^public", x, ignore.case = TRUE) ~ "Public",
      grepl("^private", x, ignore.case = TRUE) ~ "Private",
      TRUE ~ NA_character_
    )
    if (any(is.na(out) & !is.na(x))) {
      stop("TX: unrecognised School Type: ",
           paste(unique(x[is.na(out) & !is.na(x)]), collapse = ", "), call. = FALSE)
    }
    out
  }

  PCT_COLS <- c("pct_dtap", "pct_tdap", "pct_menacwy", "pct_hep_a", "pct_hep_b",
                "pct_mmr", "pct_polio", "pct_varicella")

  # Parse the antigen columns present, recording the "NR" marker per measure.
  parse_antigens <- function(d) {
    for (col in intersect(PCT_COLS, names(d))) {
      raw <- no_report(d[[col]])
      d[[sub("^pct_", "flag_", col)]] <- censor_flag(raw)
      d[[col]] <- clean_numeric(raw)
    }
    d
  }

  coverage_files <- list.files("raw/coverage", pattern = "\\.xlsx?$", full.names = TRUE)
  if (!length(coverage_files)) {
    stop("TX: no coverage workbooks in raw/coverage/ to process.", call. = FALSE)
  }

  coverage <- bind_rows(lapply(coverage_files, function(path) {
    meta <- coverage_meta(path)
    counties <- read_coverage_sheet(path, sheet_named(path, "county")) %>%
      mutate(type = "county")
    districts <- read_coverage_sheet(path, sheet_named(path, "district")) %>%
      mutate(type = "district", district = trimws(district),
             school_type = school_type_label(school_type))
    bind_rows(counties, districts) %>%
      mutate(time = as.Date(school_year_time(meta$start)), grade = meta$grade)
  })) %>%
    parse_antigens()

  # The county sheet's total row is labelled "Texas". Any other label that is
  # not a county stops the run here.
  coverage <- coverage %>%
    join_county_fips("TX", statewide = c("Texas", "State", "Total")) %>%
    mutate(type = if_else(type == "county" & nchar(geography) == 2L, "state", type))

  # ---- Conscientious exemptions ---------------------------------------------
  # Sheet names carry the grade; the header row carries one school-year label
  # per column (one issue prints "2022-2023%", which
  # school_year_end_from_label() reads past). Rows after the counties are
  # footnotes beginning with "*".
  exemption_grade <- function(sheet) {
    s <- tolower(sheet)
    if (grepl("kinder", s)) return("Kindergarten")
    if (grepl("7|seventh", s)) return("7th grade")
    if (grepl("k\\s*-?\\s*12", s)) return("K-12")
    stop("TX: unrecognised exemption sheet '", sheet, "'", call. = FALSE)
  }

  read_exemption_sheet <- function(path, sheet) {
    d <- read_sheet(path, sheet)
    hdr <- header_row(d)
    header <- trimws(as.character(unlist(d[hdr, ])))
    end_year <- school_year_end_from_label(header)
    year_cols <- which(!is.na(end_year))
    if (!length(year_cols)) {
      stop("TX: no school-year columns in ", basename(path), " / ", sheet, call. = FALSE)
    }
    body <- d[seq_len(nrow(d)) > hdr, c(1L, year_cols), drop = FALSE]
    names(body) <- c("county", as.character(end_year[year_cols] - 1L))
    body %>%
      mutate(county = trimws(county)) %>%
      filter(!is.na(county), county != "", !grepl("^\\*", county)) %>%
      pivot_longer(-county, names_to = "start", values_to = "raw") %>%
      mutate(
        grade = exemption_grade(sheet),
        time = as.Date(school_year_time(start)),
        raw = no_report(raw),
        flag_conscientious_exemption = censor_flag(raw),
        pct_conscientious_exemption = clean_numeric(raw)
      ) %>%
      select(-start, -raw)
  }

  exemption_files <- list.files("raw/exemptions", pattern = "\\.xlsx?$", full.names = TRUE)
  if (!length(exemption_files)) {
    stop("TX: no exemption workbooks in raw/exemptions/ to process.", call. = FALSE)
  }

  # Newest issue first, so distinct() keeps its value for a county-year that
  # several issues carry.
  issue_start <- as.integer(str_extract(basename(exemption_files), "^\\d{4}"))
  if (anyNA(issue_start)) {
    stop("TX: exemption workbook name without a leading year: ",
         paste(basename(exemption_files)[is.na(issue_start)], collapse = ", "),
         call. = FALSE)
  }
  exemption_files <- exemption_files[order(issue_start, decreasing = TRUE)]

  exemptions <- bind_rows(lapply(exemption_files, function(path) {
    bind_rows(lapply(readxl::excel_sheets(workbook_path(path)), function(sheet) {
      read_exemption_sheet(path, sheet)
    }))
  })) %>%
    distinct(county, grade, time, .keep_all = TRUE) %>%
    join_county_fips("TX", statewide = c("Texas", "State", "Total")) %>%
    mutate(type = if_else(nchar(geography) == 2L, "state", "county"))

  # ---- Assemble ----------------------------------------------------------------
  county_rows <- coverage %>%
    filter(type != "district") %>%
    select(-facility_id, -school_type, -district, -address, -county) %>%
    full_join(exemptions %>% select(-county),
              by = c("time", "geography", "geography_name", "grade", "type")) %>%
    select(time, geography, geography_name, type, grade,
           any_of(c(rbind(PCT_COLS, sub("^pct_", "flag_", PCT_COLS)))),
           pct_conscientious_exemption, flag_conscientious_exemption) %>%
    arrange(time, grade, type, geography)

  district_rows <- coverage %>%
    filter(type == "district") %>%
    select(time, geography, geography_name, type, district, school_type, grade,
           any_of(c(rbind(PCT_COLS, sub("^pct_", "flag_", PCT_COLS))))) %>%
    arrange(time, grade, geography, district)

  message(sprintf(
    "TX: %d county/state rows and %d district rows, school years %s to %s",
    nrow(county_rows), nrow(district_rows),
    min(county_rows$time), max(county_rows$time)))

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(county_rows, "Texas", "./standard/data.csv.gz", from = "rate")
  write_standard(district_rows, "Texas districts", "./standard/data_districts.csv.gz",
                 from = "rate")
  update_latest_year(latest_school_year(out))

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
