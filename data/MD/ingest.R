library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(readr)
library(vroom)
source("../../resources/add_state_column.R")

# =============================================================================
# MD - Kindergarten Immunization & Exemption Rates (county-level)
# Source: Maryland DoH Center for Immunization, "Percent of Kindergarteners
#   Vaccinated by School" workbooks (one per school year), linked from
#   https://health.maryland.gov/phpa/OIDEOR/IMMUN/Pages/Kindergarten_Immunization_Rates_by_School.aspx
# The published county tables are PDF-only; these by-school Excel files carry
# per-school enrollment, so we aggregate to county (enrollment-weighted),
# self-updating as new years are posted. Output schema unchanged.
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
  wmean <- function(x, w) {
    ok <- !is.na(x) & !is.na(w)
    if (!any(ok) || sum(w[ok]) == 0) return(NA_real_)
    sum(x[ok] * w[ok]) / sum(w[ok])
  }

  process_school_file <- function(path) {
    m <- str_match(basename(path), "(20\\d{2})-(20\\d{2})")
    if (is.na(m[1, 1])) return(NULL)
    time <- as.Date(paste0(m[1, 3], "-09-01"))  # school-year END year, Sept 1

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
      enroll = num(enroll_col),
      surveyed = num(gv("with records")),
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

  agg <- school %>%
    group_by(county, time) %>%
    summarise(
      N_enroll = sum(enroll, na.rm = TRUE),
      N_surveyed = sum(surveyed, na.rm = TRUE),
      pct_dtap = wmean(pct_dtap, enroll),
      pct_polio = wmean(pct_polio, enroll),
      pct_mmr = wmean(pct_mmr, enroll),
      pct_hep_b = wmean(pct_hep_b, enroll),
      pct_varicella = wmean(pct_varicella, enroll),
      pct_medical_exempt = wmean(pct_medical, enroll),
      pct_personal_exempt = wmean(pct_religious, enroll),
      .groups = "drop"
    ) %>%
    mutate(
      pct_full_exempt = pct_medical_exempt + pct_personal_exempt,
      grade = "Kindergarten"
    )

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_md <- all_fips %>%
    filter(state == "MD", nchar(geography) == 5) %>%
    mutate(join_key = tolower(gsub(" [Cc]ounty$", "", geography_name))) %>%
    select(geography, fips_name = geography_name, join_key)

  data_out <- agg %>%
    mutate(join_key = tolower(gsub(" [Cc]ounty$", "", county))) %>%
    left_join(fips_md, by = "join_key") %>%
    filter(!is.na(geography)) %>%
    mutate(
      geography_name = fips_name,
      N_dtap = NA_real_, N_polio = NA_real_, N_mmr = NA_real_,
      N_hep_b = NA_real_, N_varicella = NA_real_,
      N_personal_exempt = NA_real_, N_medical_exempt = NA_real_, N_full_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      N_enroll, N_surveyed
    ) %>%
    arrange(time, geography_name)

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(add_state_column(data_out, "Maryland"), "./standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
