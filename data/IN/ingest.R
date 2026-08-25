library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")

# =============================================================================
# IN - School Immunization Coverage by County & Grade
# Source: Indiana DOH, "Immunization Division's School Supplemental Dashboard"
#   on the state open-data hub (CKAN). We query the CKAN package API to enumerate
#   every per-school-year workbook and download each, so the series self-updates
#   as IDOH posts new years (2023-24, 2024-25, 2025-26, ...).
#   Dataset: https://hub.mph.in.gov/dataset/immunization-division-s-school-supplemental-dashboard
#
# NOTE ON METRICS: this hub file reports per-antigen COVERAGE (DTaP, Polio, MMR,
#   HepB, Varicella, Tdap, HepA, MCV4) and CHIRP enrollment at the school level,
#   which we enrollment-weight up to the county. It does NOT carry the
#   medical/religious EXEMPTION split that the previous committed snapshot had —
#   IDOH does not publish an exemption file on the hub — so the exemption columns
#   are left NA here. Coverage is populated for the first time in exchange.
#
# LAYOUT DRIFT: headers vary by year — 2023-24 lacks County_Code (county name
#   only) and uses spaces in rate headers ("MMR Rate"); 2024-25 names DTaP
#   "Dtap/Td_Rate"; 2025-26 uses "Dtap_Rate". Columns are resolved by pattern.
# =============================================================================

ua <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
pkg_id <- "f4e11e6e-2e4f-41b1-a46c-c6d43c6ce1a3"
api_url <- paste0("https://hub.mph.in.gov/api/3/action/package_show?id=", pkg_id)

dir.create("raw", showWarnings = FALSE)

# ---- Discover & download every per-year school workbook via the CKAN API ----
resp <- httr::GET(api_url, httr::user_agent(ua), httr::timeout(120))
pkg <- jsonlite::fromJSON(rawToChar(httr::content(resp, "raw")), simplifyDataFrame = TRUE)
resources <- pkg$result$resources

data_res <- resources[
  grepl("immunization-data_school-year", resources$url, ignore.case = TRUE), ,
  drop = FALSE
]

for (i in seq_len(nrow(data_res))) {
  url <- data_res$url[i]
  yr <- str_extract(url, "\\d{4}-\\d{4}")
  dest <- file.path("raw", paste0("school-year-", yr, ".xlsx"))
  try({
    r <- httr::GET(url, httr::user_agent(ua), httr::timeout(180))
    if (httr::status_code(r) == 200) {
      writeBin(httr::content(r, "raw"), dest)
    }
  }, silent = TRUE)
}

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # Antigen rate columns are 0-1 proportions in the source; resolved by pattern
  # so header drift across years (spaces vs "_", "Dtap/Td_Rate" vs "Dtap_Rate").
  rate_patterns <- list(
    pct_dtap      = "^Dtap.*Rate$",
    pct_polio     = "IPV.*Rate",
    pct_mmr       = "^MMR.*Rate$",
    pct_hep_b     = "HepB.*Rate",
    pct_varicella = "VAR.*Rate",
    pct_tdap      = "^Tdap.*Rate$",
    pct_hep_a     = "HepA.*Rate",
    pct_mcv4      = "MCV4.*Rate"
  )

  resolve <- function(nms, pattern) {
    hit <- grep(pattern, nms, ignore.case = TRUE, value = TRUE, perl = TRUE)
    if (length(hit)) hit[1] else NA_character_
  }
  name_key <- function(x) toupper(gsub("[^A-Za-z]", "", as.character(x)))
  grade_label <- function(g) {
    g <- as.character(g)
    ifelse(g == "K", "Kindergarten", paste0("Grade ", g))
  }

  process_year <- function(path) {
    yr <- str_extract(basename(path), "\\d{4}-\\d{4}")
    year_end <- str_extract(yr, "\\d{4}$")
    time <- as.Date(school_year_time_from_end(year_end))

    d <- readxl::read_excel(path, sheet = 1, .name_repair = "unique")
    names(d) <- gsub("\\s+", "_", trimws(names(d)))
    nms <- names(d)

    enroll_col <- resolve(nms, "DOE_Enrollment")
    d$.enroll <- readr::parse_number(as.character(d[[enroll_col]]))
    d$.code <- if ("County_Code" %in% nms) {
      str_pad(as.character(d[["County_Code"]]), 5, pad = "0")
    } else NA_character_
    d$.cname <- if ("County" %in% nms) name_key(d[["County"]]) else NA_character_
    d$.grade <- as.character(d[["Grade"]])

    for (out_col in names(rate_patterns)) {
      src <- resolve(nms, rate_patterns[[out_col]])
      d[[out_col]] <- if (!is.na(src)) as.numeric(d[[src]]) * 100 else NA_real_
    }

    d %>%
      filter(!is.na(.grade)) %>%
      group_by(.code, .cname, .grade) %>%
      summarize(
        across(all_of(names(rate_patterns)),
               ~ weighted.mean(.x, .enroll, na.rm = TRUE)),
        N_enrolled = sum(.enroll, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(time = time)
  }

  year_files <- list.files("./raw", pattern = "^school-year-\\d{4}-\\d{4}\\.xlsx$",
                           full.names = TRUE)
  data_clean <- bind_rows(lapply(year_files, process_year))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  in_fips <- all_fips %>%
    filter(state == "IN", nchar(geography) == 5) %>%
    transmute(
      geography,
      geography_name = gsub(" County", "", geography_name),
      .name_key = name_key(gsub(" County", "", geography_name))
    )

  # Guarantee every standard antigen column exists even if a year lacked it.
  for (col in names(rate_patterns)) {
    if (!col %in% names(data_clean)) data_clean[[col]] <- NA_real_
  }

  data_out <- data_clean %>%
    # Prefer the file's County_Code (5-digit FIPS); fall back to name match.
    left_join(in_fips %>% select(.name_key, geo_by_name = geography),
              by = c(".cname" = ".name_key")) %>%
    mutate(geography = coalesce(.code, geo_by_name)) %>%
    filter(!is.na(geography)) %>%
    left_join(in_fips %>% select(geography, geography_name), by = "geography") %>%
    mutate(
      grade = grade_label(.grade),
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
      N_enrolled,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      pct_tdap, pct_hep_a, pct_mcv4
    ) %>%
    arrange(time, geography_name, grade)

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "Indiana", "./standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
