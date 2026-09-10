source("../../resources/rate_scale.R")
source("../../resources/fetch.R")
# =============================================================================
# MA - School Immunization & Exemption Rates by County (Kindergarten & Grade 7)
# =============================================================================
# The MDPH publishes one "immunization and exemption rates by county" workbook
# per school year. Current-year files live on the School Immunizations page and
# prior years on the archive page. We scrape both, download every by-county
# Kindergarten / Grade 7 workbook, and parse them into one long series, so the
# data self-updates as new school years are posted.
#
# NOTE: www.mass.gov sits behind a WAF that returns a "Not allowed" HTML page to
# non-browser clients. Setting only a User-Agent is NOT enough (download.file
# would silently save the block page); we must present the full browser header
# set that browser_headers() in resources/fetch.R carries (Accept, Accept-
# Language, Sec-Fetch-*, Upgrade-Insecure-Requests). The /doc/.../download
# endpoints also expect a same-origin Referer. The WAF also blocks datacenter
# IP ranges outright, so CI cannot reach mass.gov at all (sources.json marks
# both entries ci_reachable = false); new years are fetched from a workstation
# and committed, and a run that cannot reach the pages proceeds on raw/.

library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
library(httr)
library(rvest)
library(xml2)

sources <- read_sources()
src_pages <- list(
  current = source_entry(sources, "mass_gov_current"),
  archive = source_entry(sources, "mass_gov_archive")
)
pages <- c(
  current = src_pages$current$page_url,
  archive = src_pages$archive$page_url
)

dir.create("raw", showWarnings = FALSE)
process <- dcf::dcf_process_record()
prev <- process$fetch_state

# ---- 1. Scrape both pages for by-county Kindergarten / Grade 7 links ---------
# Either page being unreachable (WAF, outage, datacenter IP) is a warning, not
# a stop: every year already fetched is committed under raw/.
links <- unique(unlist(lapply(names(pages), function(p) {
  hits <- discover_links(
    pages[[p]], src_pages[[p]]$pattern,
    exclude = "by-school|three-year|combined|program",
    must_find = FALSE, headers = browser_headers()
  )
  hits$url
}), use.names = FALSE))

# Derive grade and school-year start from the slug. Two slug patterns exist:
#   YYYY-YYYY-<grade>-...-by-county           (recent years)
#   <grade>-...-by-county-YYYY-YYYY[-N]       (older years, trailing dup suffix)
slug_grade <- function(u) {
  ifelse(grepl("kindergarten", u, ignore.case = TRUE), "Kindergarten",
    ifelse(grepl("grade-7", u, ignore.case = TRUE), "7th grade", NA_character_)
  )
}
slug_year <- function(u) {
  m <- str_match(u, "(20\\d{2})[-/](?:20\\d{2}|\\d{2})")
  as.integer(m[, 2])
}

# ---- 2. Download workbooks we do not already have ----------------------------
# The per-year files are immutable once published, so fetch_file() is called
# with if_exists = "skip": a year whose workbook is already on disk and opens
# costs no request. In steady state that is zero workbook requests and only the
# two page requests above. This matters: the mass.gov WAF rate-limits
# aggressively and will 403 an IP that bursts many requests, so before any
# download we warm up with a page visit, space requests out, and back off on
# failure (whatever is missed is retried on the next run). A block page served
# as HTTP 200 fails fetch_file()'s workbook validation and never reaches raw/.
is_workbook <- function(path) {
  file.exists(path) &&
    tryCatch(length(readxl::excel_sheets(path)) > 0, error = function(e) FALSE)
}

if (length(links)) {
  meta <- tibble(url = links, grade = slug_grade(links), year_start = slug_year(links)) %>%
    filter(!is.na(grade), !is.na(year_start)) %>%
    arrange(grade, year_start) %>%
    distinct(grade, year_start, .keep_all = TRUE) %>%
    mutate(dest = file.path("raw", sprintf(
      "MA_%s_by_county_%d-%d.xlsx",
      if_else(grade == "Kindergarten", "kindergarten", "grade7"),
      year_start, year_start + 1L
    )))

  if (!all(vapply(meta$dest, is_workbook, logical(1)))) {
    tryCatch(GET(pages[["archive"]], add_headers(.headers = browser_headers())),
             error = function(e) NULL) # warm up WAF cookie
  }
  recs <- vector("list", nrow(meta))
  for (i in seq_len(nrow(meta))) {
    recs[[i]] <- fetch_file(
      meta$url[i], meta$dest[i], type = "xlsx",
      headers = browser_headers(referer = pages[["archive"]]),
      if_exists = "skip", retries = 3L, backoff = c(5, 10, 15),
      previous = prev[[meta$dest[i]]]
    )
    if (i < nrow(meta) && recs[[i]]$status != "skipped") {
      Sys.sleep(3) # be polite between requests
    }
  }
  st <- vapply(recs, function(r) r$status, character(1))
  message("MA fetch: ", length(st), " file(s): ",
          paste(names(table(st)), table(st), collapse = ", "))
  process <- record_fetch(process, recs)
}

# ---- 3. Gate reprocessing on raw-file / script changes -----------------------
raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
  !identical(process$script_hash, script_hash)) {

  parse_num <- function(x) readr::parse_number(as.character(x))

  # Map a raw column header to a canonical role, tolerant of the naming drift
  # across eras ("3 Hep B"/"3 HEPB", "2 Varicella"/"Immunity to Chickenpox",
  # "Medical Exemption"/"Medical Exemptions", embedded newlines, footnote *).
  classify_header <- function(h) {
    x <- gsub("[^a-z0-9]", "", tolower(h))
    if (is.na(x) || x == "") return(NA_character_)
    if (x == "county") return("county")
    if (grepl("children", x)) return("n_enrolled")
    if (grepl("medical", x) && grepl("exempt", x)) return("medical_exempt")
    if (grepl("religious", x) && grepl("exempt", x)) return("religious_exempt")
    if (grepl("total", x) && grepl("exempt", x)) return("full_exempt")
    if (grepl("dtap", x)) return("dtap")
    if (grepl("polio", x)) return("polio")
    if (grepl("mmr", x)) return("mmr")
    if (grepl("hepb", x)) return("hep_b")
    if (grepl("varicella", x) || grepl("chickenpox", x)) return("varicella")
    if (grepl("tdap", x)) return("tdap")
    if (grepl("menacwy", x)) return("menacwy")
    NA_character_
  }

  # Return the county data as a data frame whose columns are canonical roles.
  # Files carry Notes/definition rows, and some years bundle school-level sheets
  # alongside the county summary, so we gather every sheet that has a "County"
  # header and pick the county table: prefer one whose sheet name says "county",
  # else the smallest such table (county tables have ~14 rows, school-level ones
  # have hundreds).
  read_county_sheet <- function(path) {
    candidates <- list()
    for (sh in readxl::excel_sheets(path)) {
      raw <- tryCatch(
        suppressMessages(readxl::read_excel(path, sheet = sh, col_names = FALSE)),
        error = function(e) NULL
      )
      if (is.null(raw) || nrow(raw) < 2) next
      hdr_row <- NA_integer_
      for (r in seq_len(min(8L, nrow(raw)))) {
        vals <- tolower(str_squish(as.character(unlist(raw[r, ]))))
        if (any(vals == "county", na.rm = TRUE)) {
          hdr_row <- r
          break
        }
      }
      if (is.na(hdr_row)) next
      roles <- vapply(as.character(unlist(raw[hdr_row, ])), classify_header, character(1))
      if (!("county" %in% roles)) next
      body <- raw[(hdr_row + 1L):nrow(raw), , drop = FALSE]
      keep <- !is.na(roles)
      body <- body[, keep, drop = FALSE]
      names(body) <- roles[keep]
      body <- body[, !duplicated(names(body)), drop = FALSE]
      n_county <- sum(!is.na(body$county) & str_squish(as.character(body$county)) != "")
      candidates[[length(candidates) + 1L]] <- list(
        body = body, n = n_county,
        name_county = grepl("county", sh, ignore.case = TRUE)
      )
    }
    if (!length(candidates)) return(NULL)
    named <- Filter(function(x) x$name_county, candidates)
    pool <- if (length(named)) named else candidates
    pool[[which.min(vapply(pool, function(x) x$n, integer(1)))]]$body
  }

  excluded_rows <- c(
    "state total", "gap", "grand total", "unimmunized", "un-immunized",
    "total", "statewide", "massachusetts"
  )

  build_file <- function(path, grade, year_start) {
    b <- read_county_sheet(path)
    if (is.null(b)) return(NULL)
    get_role <- function(role) if (role %in% names(b)) b[[role]] else NA
    # MDPH is not consistent between workbooks: 25 of the 26 by-county files
    # publish proportions, while MA_grade7_by_county_2019-2020.xlsx publishes
    # percent points (MMR 94.7-100). So the scale is settled per file, and only
    # from the coverage antigens -- never from the exemption columns, whose
    # magnitude cannot distinguish the two scales. detect_scale_from_coverage()
    # errors rather than guessing if a file falls between the two.
    file_scale <- detect_scale_from_coverage(
      get_role("dtap"), get_role("polio"), get_role("mmr"),
      get_role("hep_b"), get_role("varicella"),
      label = basename(path)
    )
    to_frac <- function(x) parse_rate(x, from = file_scale)
    tibble(
      county = str_squish(gsub("[*0-9]", "", as.character(get_role("county")))),
      grade = grade,
      year_start = year_start,
      N_enrolled = parse_num(get_role("n_enrolled")),
      pct_dtap = to_frac(get_role("dtap")),
      pct_polio = to_frac(get_role("polio")),
      pct_mmr = to_frac(get_role("mmr")),
      pct_hep_b = to_frac(get_role("hep_b")),
      pct_varicella = to_frac(get_role("varicella")),
      pct_tdap = to_frac(get_role("tdap")),
      pct_menacwy = to_frac(get_role("menacwy")),
      pct_medical_exempt = to_frac(get_role("medical_exempt")),
      pct_religious_exempt = to_frac(get_role("religious_exempt")),
      pct_full_exempt = to_frac(get_role("full_exempt"))
    ) %>%
      filter(!is.na(county), !(tolower(county) %in% excluded_rows), county != "")
  }

  # Only the per-year workbooks this script produces (MA_<grade>_by_county_...).
  raw_files <- list.files("raw",
    pattern = "^MA_(kindergarten|grade7)_by_county_20\\d{2}-\\d{4}\\.xlsx$",
    full.names = TRUE
  )
  file_grade <- ifelse(grepl("kindergarten", basename(raw_files)), "Kindergarten", "7th grade")
  file_year <- as.integer(str_match(basename(raw_files), "(20\\d{2})-\\d{4}")[, 2])

  data_all <- bind_rows(lapply(seq_along(raw_files), function(i) {
    if (is.na(file_year[i])) return(NULL)
    build_file(raw_files[i], file_grade[i], file_year[i])
  }))

  if (nrow(data_all) == 0) {
    stop("MA: no county workbooks available in raw/ to process.")
  }
  data_all <- data_all %>% mutate(time = as.Date(paste0(year_start, "-09-01")))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "MA", nchar(geography) == 5) %>%
    mutate(geography_name = gsub(" County", "", geography_name)) %>%
    select(geography, geography_name)

  data_out <- data_all %>%
    left_join(fips_df, by = c("county" = "geography_name")) %>%
    filter(!is.na(geography), !is.na(time)) %>%
    mutate(
      N_dtap = NA_real_, N_polio = NA_real_, N_mmr = NA_real_,
      N_hep_b = NA_real_, N_varicella = NA_real_,
      N_religious_exempt = NA_real_, N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_, N_tdap = NA_real_, N_menacwy = NA_real_
    ) %>%
    arrange(grade, time, county) %>%
    transmute(
      time, geography, geography_name = county, grade, N_enrolled,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_religious_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_religious_exempt, pct_medical_exempt, pct_full_exempt,
      N_tdap, pct_tdap, N_menacwy, pct_menacwy
    )

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(data_out, "Massachusetts", "./standard/data.csv.gz", from = "rate")
  update_latest_year(latest_school_year(out))

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
