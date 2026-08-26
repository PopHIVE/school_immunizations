library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(readr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")

# =============================================================================
# MD - Kindergarten Immunization & Exemption Rates (school- and county-level)
# Source: Maryland DoH Center for Immunization, "Percent of Kindergarteners
#   Vaccinated by School" workbooks (one per school year), linked from
#   https://health.maryland.gov/phpa/OIDEOR/IMMUN/Pages/Kindergarten_Immunization_Rates_by_School.aspx
# The published county tables are PDF-only; these by-school Excel files carry
# per-school enrollment, so we keep the per-school rows (type = "school") and
# also aggregate to county (enrollment-weighted, type = "county"), self-
# updating as new years are posted -- same school/county split as HI.
# =============================================================================

options(HTTPUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")
md_host <- "https://health.maryland.gov"
dir.create("raw", showWarnings = FALSE)

# ---- Download by-school workbooks ----
local({
  page <- paste0(md_host, "/phpa/OIDEOR/IMMUN/Pages/Kindergarten_Immunization_Rates_by_School.aspx")
  tmp <- tempfile(fileext = ".html")
  if (tryCatch({ download.file(page, tmp, quiet = TRUE); TRUE }, error = function(e) FALSE)) {
    html <- paste(readLines(tmp, warn = FALSE), collapse = "\n")
    hrefs <- unlist(str_extract_all(html, 'href="[^"]*[Vv]accinated[^"]*\\.xlsx"'))
    hrefs <- str_replace_all(hrefs, 'href="|"$', "")
    hrefs <- unique(hrefs[str_detect(hrefs, "^/phpa/")])  # health.maryland.gov-hosted only
    for (h in hrefs) {
      dest <- file.path("raw", utils::URLdecode(basename(h)))
      try(download.file(paste0(md_host, gsub(" ", "%20", h)), dest, mode = "wb", quiet = TRUE),
          silent = TRUE)
    }
  }
})

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # as.numeric (not parse_number) so scientific-notation proportions like
  # "1.77E-2" and redacted "**" cells are handled correctly.
  num <- function(x) suppressWarnings(as.numeric(gsub(",", "", as.character(x))))
  rescale_pct <- function(x) {
    if (all(is.na(x))) return(x)
    if (max(x, na.rm = TRUE) <= 1.5) x * 100 else x
  }

  process_school_file <- function(path) {
    m <- str_match(basename(path), "(20\\d{2})-(20\\d{2})")
    if (is.na(m[1, 1])) return(NULL)
    # The filename carries the school year as a range; m[1, 3] is its END year,
    # so the start year it is dated to comes from school_year_time_from_end().
    time <- as.Date(school_year_time_from_end(m[1, 3]))

    # The data sheet is named "Kindergarten" in most years but "Sheet1" in
    # others (with a separate "Notes" cover sheet). Pick whichever sheet has
    # the "School Name" header.
    data_sheet <- NA_character_
    for (s in excel_sheets(path)) {
      probe <- suppressMessages(read_excel(path, sheet = s, col_names = FALSE, n_max = 15))
      if (any(str_detect(as.character(probe[[1]]), regex("school name", ignore_case = TRUE)), na.rm = TRUE)) {
        data_sheet <- s
        break
      }
    }
    if (is.na(data_sheet)) return(NULL)

    raw <- suppressMessages(read_excel(path, sheet = data_sheet, col_names = FALSE))
    hdr <- which(str_detect(as.character(raw[[1]]), regex("^\\s*School Name", ignore_case = TRUE)))[1]
    if (is.na(hdr)) return(NULL)
    labels <- tolower(str_squish(as.character(unlist(raw[hdr, ], use.names = FALSE))))
    df <- raw[(hdr + 1):nrow(raw), , drop = FALSE]
    names(df) <- paste0("V", seq_len(ncol(df)))

    gv <- function(pat) {
      i <- which(str_detect(labels, pat))[1]
      if (is.na(i)) rep(NA, nrow(df)) else df[[paste0("V", i)]]
    }
    # Enrollment column label varies by year ("TOTAL K Students" vs "Total
    # Number of Enrolled Kindergarten Students"); match total+student but not
    # the WITH/WITHOUT-records breakdown columns.
    enroll_i <- which(str_detect(labels, "total") & str_detect(labels, "student") &
                      !str_detect(labels, "with"))[1]
    enroll_col <- if (is.na(enroll_i)) rep(NA, nrow(df)) else df[[paste0("V", enroll_i)]]

    out <- tibble(
      county = str_squish(as.character(gv("^county$"))),
      school_name = str_squish(as.character(gv("^school name"))),
      school_type = str_squish(as.character(gv("^type of school"))),
      enroll = num(enroll_col),
      pct_medical = num(gv("medical exemption")),
      pct_religious = num(gv("religious exemption")),
      pct_dtap = num(gv("dtap")),
      pct_polio = num(gv("polio")),
      pct_mmr = num(gv("mmr")),
      pct_hep_b = num(gv("hep")),
      pct_varicella = num(gv("varicella")),
      time = time
    ) %>%
      filter(!is.na(county), county != "", tolower(county) != "county")

    # Scale varies by year: 2019-2024 files store proportions (0-1), 2025-26
    # stores percent points. Detect from the coverage columns and normalize the
    # whole file to percent points before aggregating.
    cov_max <- suppressWarnings(max(c(out$pct_dtap, out$pct_mmr, out$pct_polio), na.rm = TRUE))
    if (is.finite(cov_max) && cov_max <= 1.5) {
      pcols <- c("pct_medical", "pct_religious", "pct_dtap", "pct_polio",
                 "pct_mmr", "pct_hep_b", "pct_varicella")
      out <- out %>% mutate(across(all_of(pcols), ~ .x * 100))
    }
    out
  }

  files <- list.files("raw", pattern = "[Vv]accinated.*\\.xlsx$", full.names = TRUE)
  school <- bind_rows(lapply(files, process_school_file))

  # County rate = sum of per-school counts / sum of per-school enrollment, not
  # an enrollment-weighted mean of the percentages -- so a school with no
  # figure for a given measure contributes nothing to that measure's numerator
  # (same treatment HI gives its per-school exemption counts).
  agg <- school %>%
    mutate(
      N_dtap = enroll * pct_dtap / 100,
      N_polio = enroll * pct_polio / 100,
      N_mmr = enroll * pct_mmr / 100,
      N_hep_b = enroll * pct_hep_b / 100,
      N_varicella = enroll * pct_varicella / 100,
      N_medical_exempt = enroll * pct_medical / 100,
      N_personal_exempt = enroll * pct_religious / 100
    ) %>%
    group_by(county, time) %>%
    summarise(
      N_enroll = sum(enroll, na.rm = TRUE),
      N_dtap = sum(N_dtap, na.rm = TRUE),
      N_polio = sum(N_polio, na.rm = TRUE),
      N_mmr = sum(N_mmr, na.rm = TRUE),
      N_hep_b = sum(N_hep_b, na.rm = TRUE),
      N_varicella = sum(N_varicella, na.rm = TRUE),
      N_medical_exempt = sum(N_medical_exempt, na.rm = TRUE),
      N_personal_exempt = sum(N_personal_exempt, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      pct_dtap = if_else(N_enroll > 0, 100 * N_dtap / N_enroll, NA_real_),
      pct_polio = if_else(N_enroll > 0, 100 * N_polio / N_enroll, NA_real_),
      pct_mmr = if_else(N_enroll > 0, 100 * N_mmr / N_enroll, NA_real_),
      pct_hep_b = if_else(N_enroll > 0, 100 * N_hep_b / N_enroll, NA_real_),
      pct_varicella = if_else(N_enroll > 0, 100 * N_varicella / N_enroll, NA_real_),
      pct_medical_exempt = if_else(N_enroll > 0, 100 * N_medical_exempt / N_enroll, NA_real_),
      pct_personal_exempt = if_else(N_enroll > 0, 100 * N_personal_exempt / N_enroll, NA_real_),
      grade = "Kindergarten"
    )

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_md <- all_fips %>%
    filter(state == "MD", nchar(geography) == 5) %>%
    mutate(
      join_key = tolower(gsub(" [Cc]ounty$", "", geography_name)),
      # The standard label is the bare county name, as join_county_fips() emits
      # for the states that use it -- not all_fips' "Allegany County".
      fips_name = sub("\\s+[Cc]ounty$", "", geography_name)
    ) %>%
    select(geography, fips_name, join_key)

  join_fips <- function(df) {
    df %>%
      mutate(join_key = tolower(gsub(" [Cc]ounty$", "", county))) %>%
      left_join(fips_md, by = "join_key") %>%
      filter(!is.na(geography)) %>%
      mutate(geography_name = fips_name)
  }

  # Per-school rows, alongside the county aggregates below -- same layout as
  # HI's school/county split (type + school_name/school_type, NA for county
  # rows).
  schools_out <- school %>%
    join_fips() %>%
    transmute(
      time, geography, geography_name, type = "school",
      school_name, school_type, grade = "Kindergarten",
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt = pct_religious, pct_medical_exempt = pct_medical,
      N_enroll = enroll
    )

  counties_out <- agg %>%
    join_fips() %>%
    mutate(type = "county", school_name = NA_character_, school_type = NA_character_) %>%
    transmute(
      time, geography, geography_name, type, school_name, school_type, grade,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt,
      N_enroll
    )

  data_out <- bind_rows(schools_out, counties_out) %>%
    arrange(time, geography_name, desc(type), school_name)

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "Maryland", "./standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
