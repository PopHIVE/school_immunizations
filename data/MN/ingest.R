source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")
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

sources <- read_sources()
src <- source_entry(sources, "mdh_county_workbooks")
process <- dcf::dcf_process_record()
prev <- process$fetch_state

dir.create("raw", showWarnings = FALSE)

# ---- Discover & download county kindergarten files ----
# The current-year page (page_url) lists this year's workbook and the archive
# page (url) the earlier ones. Neither listing blocks the other: raw/ is
# committed, so a page that is down is a warning and the run goes on with
# whatever the other page lists plus what is on disk. MDH revises the
# current-year workbook in place, so every discovered file is re-fetched
# (conditionally, on ETag / Last-Modified) rather than skipped when present.
# fetch.R sends the browser header set MDH requires (it 403s bare clients).
links <- unique(c(
  discover_links(src$page_url, src$pattern, must_find = FALSE)$url,
  discover_links(src$url, src$pattern, must_find = FALSE)$url
))
recs <- fetch_many(links,
  dest_fn = function(u) file.path("raw", basename(u)),
  if_exists = "replace", previous = prev
)
process <- record_fetch(process, recs)

raw_state <- raw_state_md5()
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
    time <- as.Date(school_year_time_from_end(end_year))

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

  # Title-casing in the source breaks McLeod, Lac qui Parle and
  # Lake of the Woods; those used to end up with no county FIPS at all.
  data_out <- data_clean %>%
    join_county_fips("MN", statewide = c("Statewide", "Minnesota", "Total")) %>%
    mutate(
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
      time, geography, geography_name, grade, enrollment,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      N_dtap, pct_dtap, pct_dtap_nonmedical, pct_dtap_medical,
      N_polio, pct_polio, pct_polio_nonmedical, pct_polio_medical,
      N_mmr, pct_mmr, pct_mmr_nonmedical, pct_mmr_medical,
      N_hep_b, pct_hep_b, pct_hep_b_nonmedical, pct_hep_b_medical,
      N_varicella, pct_varicella, pct_varicella_nonmedical, pct_varicella_medical,
      pct_varicella_disease_history
    ) %>%
    arrange(time, geography_name)

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(data_out, "Minnesota", "standard/data.csv.gz", from = "percent")
  update_latest_year(latest_school_year(out))

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
