source("../../resources/add_state_column.R")
# =============================================================================
# MN - Kindergarten Vaccination Coverage by County
# Source: MN Dept of Health, Annual Immunization Status Report (school data).
#   County kindergarten files are published as `kcounty<YY><YY>.xlsx` and linked
#   from the current-year page and the archive page. We scrape those listings and
#   download every county file, so the series self-updates as MN posts new years.
#   https://www.health.state.mn.us/people/immunize/stats/school/index.html
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readxl)
library(readr)
library(vroom)

# MDH blocks non-browser user agents (HTTP 403); present a browser UA.
options(HTTPUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

base_url <- "https://www.health.state.mn.us/people/immunize/stats/school/"
dir.create("raw", showWarnings = FALSE)

# ---- Discover & download county kindergarten files ----
discover_codes <- function(pages) {
  codes <- character()
  for (pg in pages) {
    tmp <- tempfile(fileext = ".html")
    ok <- tryCatch({ download.file(paste0(base_url, pg), tmp, quiet = TRUE); TRUE },
                   error = function(e) FALSE)
    if (ok) {
      html <- paste(readLines(tmp, warn = FALSE), collapse = "\n")
      codes <- c(codes, unlist(str_extract_all(html, "kcounty\\d{4}\\.xlsx")))
    }
  }
  unique(codes)
}
files <- discover_codes(c("index.html", "archive.html"))
for (fn in files) {
  try(download.file(paste0(base_url, fn), file.path("raw", fn), mode = "wb", quiet = TRUE),
      silent = TRUE)
}

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_pct_points <- function(x) {
  y <- readr::parse_number(as.character(x))
  if (all(is.na(y))) return(y)
  if (max(y, na.rm = TRUE) <= 1) return(y * 100)
  y
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  process_kcounty <- function(path) {
    m <- str_match(basename(path), "kcounty(\\d{2})(\\d{2})")
    if (is.na(m[1, 1])) return(NULL)
    end_year <- as.integer(paste0("20", m[1, 3]))
    time <- as.Date(paste0(end_year, "-09-01"))

    raw <- readxl::read_excel(path, sheet = "K_County", col_names = FALSE)
    header <- str_replace_all(as.character(unlist(raw[2, ], use.names = FALSE)), "\\s+", " ")
    df <- raw[-c(1, 2), ]
    names(df) <- header
    df <- df %>% mutate(across(everything(), as.character))

    pick <- function(nm) if (nm %in% names(df)) df[[nm]] else rep(NA_character_, nrow(df))

    tibble(
      county = str_to_title(str_trim(pick("County"))),
      enrollment = readr::parse_number(pick("Kindergarten Enrollment")),
      pct_dtap = parse_pct_points(pick("DTaP % Vaccinated")),
      pct_polio = parse_pct_points(pick("Polio % Vaccinated")),
      pct_mmr = parse_pct_points(pick("MMR % Vaccinated")),
      pct_hep_b = parse_pct_points(pick("Hep B % Vaccinated")),
      pct_varicella = parse_pct_points(pick("Varicella % Vaccinated")),
      pct_dtap_nonmedical = parse_pct_points(pick("DTaP % non-medical")),
      pct_dtap_medical = parse_pct_points(pick("DTaP % medical")),
      pct_polio_nonmedical = parse_pct_points(pick("Polio % non-medical")),
      pct_polio_medical = parse_pct_points(pick("Polio % medical")),
      pct_mmr_nonmedical = parse_pct_points(pick("MMR % non-medical")),
      pct_mmr_medical = parse_pct_points(pick("MMR % medical")),
      pct_hep_b_nonmedical = parse_pct_points(pick("Hep B % non-medical")),
      pct_hep_b_medical = parse_pct_points(pick("Hep B % medical")),
      pct_varicella_nonmedical = parse_pct_points(pick("Varicella % non-medical")),
      pct_varicella_medical = parse_pct_points(pick("Varicella % medical")),
      pct_varicella_disease_history = parse_pct_points(pick("Varicella % Disease History"))
    ) %>%
      filter(!is.na(county), !county %in% c("", "Na")) %>%
      mutate(time = time, grade = "Kindergarten")
  }

  kcounty_files <- list.files("./raw", pattern = "^kcounty\\d{4}\\.xlsx$", full.names = TRUE)
  data_clean <- bind_rows(lapply(kcounty_files, process_kcounty))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "MN") %>%
    mutate(geography_name = gsub(" County$", "", geography_name))

  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)

  data_out <- data_clean %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = c("county" = "geography_name")
    ) %>%
    mutate(
      geography = if_else(county %in% c("Statewide", "Minnesota", "Total"), state_fips[1], geography),
      geography_name = county,
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_,
      pct_full_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      enrollment,
      pct_dtap_nonmedical, pct_dtap_medical,
      pct_polio_nonmedical, pct_polio_medical,
      pct_mmr_nonmedical, pct_mmr_medical,
      pct_hep_b_nonmedical, pct_hep_b_medical,
      pct_varicella_nonmedical, pct_varicella_medical,
      pct_varicella_disease_history
    ) %>%
    arrange(time, geography_name)

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(add_state_column(data_out, "Minnesota"), "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
