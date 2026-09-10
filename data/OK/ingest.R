library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")

# =============================================================================
# OK - Kindergarten Immunization Survey, county tables and school-level results
# Source: Oklahoma State Department of Health (OSDH), "Immunization Survey /
#   Shot Records" page. Two kinds of workbook are linked there:
#
#   * One county summary table per school year (21-22 onward; the 19-20 and
#     20-21 tables were taken down and survive only as the committed copies in
#     raw/). Every row is a county, plus a STATEWIDE row, with the share of
#     kindergarteners up to date per antigen, up to date for all six, and the
#     share holding a medical, non-medical, or any exemption. OSDH renamed the
#     files in 2024-25 (KSurvey_OSDH_CountySummaryTable<YY-YY> ->
#     KGSCountyRates<YYYYYY>), so the dest name is derived from whichever year
#     spelling the filename carries.
#   * One school-level workbook covering every survey year since 2017-18, one
#     sheet per year, replaced in place each summer with a new year appended
#     (..._17-25 became ..._17-26). It is stored under a fixed name so the next
#     edition overwrites it. Each row is a school with district, city, county
#     and school type, the same up-to-date shares as the county table, and the
#     share with at least one exemption form on file. Medical and non-medical
#     exemptions are not split at school level.
#
#   Both publish proportions (0.90, not 90). "NR" marks a school that did not
#   respond to the survey and "*" one that withheld its figures for having
#   fewer than 10 kindergarteners; both become NA. County rows in the school
#   workbook are not aggregated here: OSDH publishes the county figures
#   itself, and the school rows carry no enrollment to weight them by.
# =============================================================================

sources <- read_sources()
county_src <- source_entry(sources, "osdh_county_tables")
school_src <- source_entry(sources, "osdh_school_level")

dir.create("raw", showWarnings = FALSE)
process <- dcf::dcf_process_record()
prev <- process$fetch_state

# A workbook that opens but has no "County" header cell is not a county table
# (oklahoma.gov has served a placeholder workbook before it serves an error).
has_county_header <- function(path) {
  raw <- readxl::read_excel(path, col_names = FALSE, n_max = 40L)
  vals <- tolower(str_squish(as.character(unlist(raw))))
  if (!any(vals == "county", na.rm = TRUE)) stop("no County header in the first 40 rows")
}

# raw/OK_County_<YY-YY>.xlsx, from either filename convention:
#   KSurvey_OSDH_CountySummaryTable21-22_protected.xlsx  -> 21-22
#   KGSCountyRates202425locked.xlsx / KGSCountyRates_202526_Protected.xlsx -> 24-25 / 25-26
county_dest <- function(u) {
  f <- basename(u)
  m <- str_match(f, "(\\d{2})-(\\d{2})")
  if (is.na(m[1, 1])) m <- str_match(f, "20(\\d{2})(\\d{2})")
  y1 <- suppressWarnings(as.integer(m[1, 2]))
  y2 <- suppressWarnings(as.integer(m[1, 3]))
  if (is.na(y1) || is.na(y2) || y2 != y1 + 1L) {
    stop("OK: cannot read a school year from county table filename '", f, "'", call. = FALSE)
  }
  file.path("raw", sprintf("OK_County_%02d-%02d.xlsx", y1, y2))
}

# The index page being down must not stop the run: raw/ is committed, so on
# failure the parse proceeds on what is on disk. The per-year county tables
# never change once posted, so an existing one that validates is not
# re-requested; the school workbook is revised in place and is always checked.
recs <- list()
county_links <- discover_links(county_src$page_url, county_src$pattern, must_find = FALSE)
if (nrow(county_links)) {
  recs <- c(recs, fetch_many(
    county_links$url, dest_fn = county_dest, type = "xlsx",
    validate = has_county_header, if_exists = "skip", previous = prev
  ))
}

school_links <- discover_links(school_src$page_url, school_src$pattern, must_find = FALSE)
if (nrow(school_links)) {
  # Should OSDH ever leave two editions linked, take the one that runs latest.
  end_year <- school_year_end_from_label(
    sub("(\\d{2})-(\\d{2})", "20\\1-20\\2", basename(school_links$url))
  )
  school_url <- school_links$url[which.max(replace(end_year, is.na(end_year), 0L))]
  school_dest <- "raw/KSurvey_OSDH_SchoolLevelResults.xlsx"
  recs <- c(recs, list(fetch_file(
    school_url, school_dest, type = "xlsx", if_exists = "replace",
    previous = prev[[school_dest]]
  )))
}
if (length(recs)) {
  st <- vapply(recs, function(r) r$status, character(1))
  message("OK fetch: ", length(st), " file(s): ",
          paste(names(table(st)), table(st), collapse = ", "))
  process <- record_fetch(process, recs)
}

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # ---- county tables --------------------------------------------------------

  county_files <- list.files("./raw", pattern = "^OK_County_\\d{2}-\\d{2}\\.xlsx$",
                             full.names = TRUE)
  if (!length(county_files)) stop("OK: no county tables in raw/")

  # Map a raw column header to a canonical role. Column ORDER is not stable
  # across years -- 19-20/20-21 publish DTaP, Hep A, Hep B, MMR, Polio,
  # Varicella while 21-22 onward publish DTaP, Polio, MMR, Hep B, Hep A,
  # Varicella -- so headers are classified by text, not position. A fixed
  # position mapping used to silently swap Polio/MMR/Hep B for the older
  # files. The 2024-25 layout ("DTaP | Polio | MMR | HepB | HepA | Varicella |
  # All Vaccines* | Medical | Non-Medical | Total**", under an "Up-to-Date % /
  # Exemption %" band) classifies the same way.
  classify_header <- function(h) {
    x <- gsub("[^a-z0-9]", "", tolower(h))
    if (is.na(x) || x == "") return(NA_character_)
    if (x == "county") return("county")
    if (grepl("^dtap", x)) return("dtap")
    if (grepl("^hepa", x)) return("hep_a")
    if (grepl("^hepb", x)) return("hep_b")
    if (grepl("^mmr", x)) return("mmr")
    if (grepl("^polio", x)) return("polio")
    if (grepl("^varicella", x)) return("varicella")
    if (grepl("^allvaccines", x)) return("all")
    if (grepl("^nonmedical", x)) return("non_medical")
    if (grepl("^medical", x)) return("medical")
    if (grepl("^total", x)) return("total")
    NA_character_
  }

  # Every workbook's data table sits below a title/legend block whose depth
  # varies by year (header row 8 in 23-24 onward, row 12 in earlier years), so
  # the header row is located by content rather than a fixed `skip`.
  read_county_table <- function(path) {
    raw <- readxl::read_excel(path, col_names = FALSE)
    hdr_row <- NA_integer_
    for (r in seq_len(nrow(raw))) {
      vals <- tolower(str_squish(as.character(unlist(raw[r, ]))))
      if (any(vals == "county", na.rm = TRUE)) {
        hdr_row <- r
        break
      }
    }
    if (is.na(hdr_row)) stop("OK: no header row found in ", path)
    roles <- vapply(as.character(unlist(raw[hdr_row, ])), classify_header, character(1))
    body <- raw[(hdr_row + 1L):nrow(raw), , drop = FALSE]
    keep <- !is.na(roles)
    body <- body[, keep, drop = FALSE]
    names(body) <- roles[keep]
    body
  }

  build_county_file <- function(path) {
    b <- read_county_table(path)
    year_match <- str_match(basename(path), "(\\d{2})-(\\d{2})")
    year_end <- paste0("20", year_match[, 3])
    time <- as.Date(school_year_time_from_end(year_end))

    # Footnote rows below the table have text in the county column and nothing
    # in DTaP; "N/A" (no school in the county responded) parses to NA too.
    tibble(
      county = str_squish(as.character(b$county)),
      time = time,
      pct_utd_dtap = clean_numeric(b$dtap),
      pct_utd_polio = clean_numeric(b$polio),
      pct_utd_mmr = clean_numeric(b$mmr),
      pct_utd_hep_b = clean_numeric(b$hep_b),
      pct_utd_hep_a = clean_numeric(b$hep_a),
      pct_utd_varicella = clean_numeric(b$varicella),
      pct_utd_all = clean_numeric(b$all),
      pct_medical = clean_numeric(b$medical),
      pct_non_medical = clean_numeric(b$non_medical),
      pct_total = clean_numeric(b$total)
    ) %>%
      filter(!is.na(county), county != "", !is.na(pct_utd_dtap))
  }

  county_all <- bind_rows(lapply(county_files, build_county_file))

  # The workbooks name counties in upper case ("ADAIR", "LE FLORE"), which no
  # longer silently falls through to the state FIPS -- see county_fips.R.
  county_out <- county_all %>%
    join_county_fips("OK", statewide = "Statewide") %>%
    mutate(
      type = if_else(nchar(geography) == 2L, "state", "county"),
      school_name = NA_character_, district = NA_character_,
      city = NA_character_, school_type = NA_character_
    )

  # ---- school-level workbook ------------------------------------------------

  school_file <- "raw/KSurvey_OSDH_SchoolLevelResults.xlsx"

  # Header text drifts between sheets ("County" / "School County"; "School
  # Type" / "Choose which of the following best describes your school:";
  # "School Name" / "****School name"), and the 2020-21 sheet appends 39 raw
  # survey-question columns ("Of the [imm_exc] kindergartners ... how many are
  # up to date* for DTaP?") that must not be mistaken for the rate columns, so
  # roles are matched on the start of the collapsed header.
  classify_school_header <- function(h) {
    x <- gsub("[^a-z0-9]", "", tolower(h))
    if (is.na(x) || x == "") return(NA_character_)
    if (grepl("^schooldistrict", x)) return("district")
    if (grepl("^schoolname", x)) return("school_name")
    if (grepl("^schoolcity", x)) return("city")
    if (grepl("^(school)?county$", x)) return("county")
    if (grepl("^schooltype$", x) || grepl("describesyourschool", x)) return("school_type")
    utd <- str_match(x, "^kindergartenersuptodatefor(dtap|polio|mmr|hepatitisb|hepatitisa|varicella|allvaccines)")[, 2]
    if (!is.na(utd)) {
      return(c(dtap = "dtap", polio = "polio", mmr = "mmr", hepatitisb = "hep_b",
               hepatitisa = "hep_a", varicella = "varicella", allvaccines = "all")[[utd]])
    }
    if (grepl("^kindergartenerswithatleastoneexemption", x)) return("total")
    NA_character_
  }

  school_roles <- c("district", "school_name", "city", "county", "school_type",
                    "dtap", "polio", "mmr", "hep_b", "hep_a", "varicella", "all", "total")

  read_school_sheet <- function(path, sheet) {
    raw <- readxl::read_excel(path, sheet = sheet, col_names = FALSE, col_types = "text")
    roles <- vapply(as.character(unlist(raw[1, ])), classify_school_header, character(1))
    missing <- setdiff(school_roles, roles)
    if (length(missing)) {
      stop("OK: sheet '", sheet, "' of ", basename(path), " lacks column(s): ",
           paste(missing, collapse = ", "))
    }
    body <- raw[-1, !is.na(roles), drop = FALSE]
    names(body) <- roles[!is.na(roles)]
    body[, !duplicated(names(body)), drop = FALSE]
  }

  # Values are carried as published. One is impossible: Lukfata PS in 2022-23
  # has 1.60 up to date for all vaccines against 0.88 for DTaP, an OSDH data
  # entry error left in the source rather than corrected here.
  build_school_sheet <- function(path, sheet) {
    b <- read_school_sheet(path, sheet)
    time <- as.Date(school_year_time_from_end(school_year_end_from_label(sheet)))
    tibble(
      district = str_squish(b$district),
      school_name = str_squish(b$school_name),
      city = str_squish(b$city),
      county = str_squish(b$county),
      school_type = str_squish(b$school_type),
      time = time,
      pct_utd_dtap = clean_numeric(b$dtap),
      pct_utd_polio = clean_numeric(b$polio),
      pct_utd_mmr = clean_numeric(b$mmr),
      pct_utd_hep_b = clean_numeric(b$hep_b),
      pct_utd_hep_a = clean_numeric(b$hep_a),
      pct_utd_varicella = clean_numeric(b$varicella),
      pct_utd_all = clean_numeric(b$all),
      pct_total = clean_numeric(b$total)
    ) %>%
      # Footnote rows ("* Denotes school reporting less than 10 ...", "NR =
      # Non-responding school") sit in the district column with no school
      # name; the 2025-2026 sheet also repeats its header row mid-table.
      filter(
        !is.na(school_name), school_name != "",
        !tolower(county) %in% c("county", "school county")
      )
  }

  school_sheets <- grep("^20\\d{2}-(20)?\\d{2}$", readxl::excel_sheets(school_file), value = TRUE)
  if (!length(school_sheets)) stop("OK: no school-year sheets in ", school_file)
  school_all <- bind_rows(lapply(school_sheets, function(sh) build_school_sheet(school_file, sh)))

  # A handful of schools have no county at all in the source (three rows
  # across 2021-22 and 2023-24). There is nothing to place them by, so they are
  # dropped rather than carried with geography = NA.
  no_county <- is.na(school_all$county) | school_all$county == ""
  if (any(no_county)) {
    message(sprintf("OK: dropping %d school row(s) with no county: %s", sum(no_county),
                    paste(unique(school_all$school_name[no_county]), collapse = "; ")))
  }

  # Epic, the statewide virtual charter, is filed under "OKLAHOMA/TULSA" in
  # 2017-18 and 2018-19. It has no single county, so it is kept with
  # geography = NA under the source label. School type is free text in the
  # older sheets ("PUBLIC", "Priate").
  school_out <- school_all %>%
    join_county_fips("OK", no_fips = "Oklahoma/Tulsa", drop_na = TRUE) %>%
    mutate(
      type = "school",
      school_type = str_to_title(school_type),
      school_type = if_else(school_type == "Priate", "Private", school_type)
    )

  data_out <- bind_rows(county_out, school_out) %>%
    mutate(grade = "Kindergarten") %>%
    transmute(
      time, geography, geography_name, type, school_name, district, city,
      school_type, grade,
      pct_utd_dtap, pct_utd_polio, pct_utd_mmr, pct_utd_hep_b, pct_utd_hep_a,
      pct_utd_varicella, pct_utd_all,
      pct_medical, pct_non_medical, pct_total
    ) %>%
    arrange(time, factor(type, levels = c("state", "county", "school")),
            geography_name, school_name)

  message(sprintf(
    "OK: %d rows (%d county, %d state, %d school), %s to %s",
    nrow(data_out), sum(data_out$type == "county"), sum(data_out$type == "state"),
    sum(data_out$type == "school"), min(data_out$time), max(data_out$time)))

  out <- write_standard(data_out, "Oklahoma", "./standard/data.csv.gz", from = "rate")
  update_latest_year(latest_school_year(out))

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
