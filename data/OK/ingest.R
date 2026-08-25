library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)

  # Map a raw column header to a canonical role. Column ORDER is not stable
  # across years -- 19-20/20-21 publish DTaP, Hep A, Hep B, MMR, Polio,
  # Varicella while 21-22 onward publish DTaP, Polio, MMR, Hep B, Hep A,
  # Varicella -- so headers are classified by text, not position. A fixed
  # position mapping used to silently swap Polio/MMR/Hep B for the older
  # files.
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
  # varies by year (header row 8 in 23-24, row 12 in earlier years), so the
  # header row is located by content rather than a fixed `skip`.
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

  build_file <- function(path) {
    b <- read_county_table(path)
    year_match <- str_match(basename(path), "(\\d{2})-(\\d{2})")
    year_end <- paste0("20", year_match[, 3])
    time <- as.Date(school_year_time_from_end(year_end))

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

  data_all <- bind_rows(lapply(raw_files, build_file))

  # The workbooks name counties in upper case ("ADAIR", "LE FLORE"), which no
  # longer silently falls through to the state FIPS -- see county_fips.R.
  data_out <- data_all %>%
    join_county_fips("OK", statewide = "Statewide") %>%
    mutate(grade = "Kindergarten") %>%
    transmute(
      time, geography, geography_name, grade,
      pct_utd_dtap, pct_utd_polio, pct_utd_mmr, pct_utd_hep_b, pct_utd_hep_a,
      pct_utd_varicella, pct_utd_all,
      pct_medical, pct_non_medical, pct_total
    )

  write_standard(data_out, "Oklahoma", "./standard/data.csv.gz", from = "rate")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
