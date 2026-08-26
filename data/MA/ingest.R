source("../../resources/rate_scale.R")
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
# would silently save the block page); we must present a full browser header set
# via httr. The /doc/.../download endpoints also expect a same-origin Referer.

library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
library(httr)
library(rvest)
library(xml2)

pages <- c(
  current = "https://www.mass.gov/info-details/school-immunizations",
  archive = "https://www.mass.gov/info-details/archive-of-school-immunization-data-and-exemption-rates"
)

browser_ua <- paste0(
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
browser_headers <- httr::add_headers(
  "User-Agent" = browser_ua,
  "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language" = "en-US,en;q=0.9",
  "Sec-Fetch-Dest" = "document",
  "Sec-Fetch-Mode" = "navigate",
  "Sec-Fetch-Site" = "none",
  "Upgrade-Insecure-Requests" = "1"
)

# ---- 1. Scrape both pages for by-county Kindergarten / Grade 7 links ---------
collect_links <- function(page_url) {
  resp <- tryCatch(GET(page_url, browser_headers), error = function(e) NULL)
  if (is.null(resp) || status_code(resp) != 200) {
    return(character())
  }
  html <- content(resp, "text", encoding = "UTF-8")
  hrefs <- xml2::xml_attr(rvest::html_elements(xml2::read_html(html), "a"), "href")
  hrefs <- hrefs[!is.na(hrefs)]
  # by-county rate workbooks only (exclude by-school, combined, other grades)
  keep <- grepl("/doc/.*(kindergarten|grade-7).*by-county.*/download", hrefs, ignore.case = TRUE) &
    !grepl("by-school|three-year|combined|program", hrefs, ignore.case = TRUE)
  hrefs[keep]
}

links <- unique(unlist(lapply(pages, collect_links), use.names = FALSE))
links <- ifelse(grepl("^https?://", links), links, paste0("https://www.mass.gov", links))
links <- unique(links)

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
# The per-year files are immutable once published, so we only fetch years that
# are missing from raw/. In steady state that is 0-1 requests. This matters:
# the mass.gov WAF rate-limits aggressively and will 403 an IP that bursts many
# requests, so we also warm up with a page visit, space requests out, and back
# off on failure (whatever is missed is retried on the next run).
dir.create("raw", showWarnings = FALSE)

is_workbook <- function(path) {
  file.exists(path) &&
    tryCatch(length(readxl::excel_sheets(path)) > 0, error = function(e) FALSE)
}

download_workbook <- function(url, dest) {
  for (attempt in seq_len(3L)) {
    tmp <- tempfile(fileext = ".xlsx")
    ok <- tryCatch({
      resp <- GET(
        url, browser_headers,
        add_headers(Referer = pages[["archive"]]),
        write_disk(tmp, overwrite = TRUE)
      )
      status_code(resp) == 200
    }, error = function(e) FALSE)
    # Guard against the WAF block page (HTTP 403/200 but not a workbook).
    valid <- ok && tryCatch(length(readxl::excel_sheets(tmp)) > 0,
      error = function(e) FALSE
    )
    if (valid) {
      file.copy(tmp, dest, overwrite = TRUE)
      unlink(tmp)
      return(TRUE)
    }
    unlink(tmp)
    Sys.sleep(5 * attempt) # back off before retrying a throttled request
  }
  FALSE
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
    ))) %>%
    filter(!vapply(dest, is_workbook, logical(1))) # skip years already downloaded

  if (nrow(meta)) {
    tryCatch(GET(pages[["archive"]], browser_headers), error = function(e) NULL) # warm up WAF cookie
    for (i in seq_len(nrow(meta))) {
      if (!download_workbook(meta$url[i], meta$dest[i])) {
        message("MA: could not download (will retry next run): ", meta$url[i])
      }
      Sys.sleep(3) # be polite between requests
    }
  }
}

# ---- 3. Gate reprocessing on raw-file / script changes -----------------------
raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
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
  write_standard(data_out, "Massachusetts", "./standard/data.csv.gz", from = "rate")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
