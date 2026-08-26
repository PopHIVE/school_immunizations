library(dcf)
library(dplyr)
library(readr)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_exempt <- function(x) {
  x <- str_squish(as.character(x))
  # Keep suppressed values ("<5") as NA so we do not overstate exemptions.
  x <- if_else(str_detect(x, "^<"), NA_character_, x)
  suppressWarnings(as.numeric(str_replace_all(x, ",", "")))
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # Every scraped file is census-tract level with the same five columns:
  # tract, county, state, population aged 4-18, exemptions aged 4-18. Only the
  # HEADERS differ -- the 23-county roll-up and 39 of the per-county files use
  # "Census,County,State,TotalPop4_18yrs,TotalExempt4_18yrs", while five
  # (Martin, Union, Volusia, Walton, Washington) carry a truncated
  # "attributes.*" header that names only three of the five columns. Reading
  # positionally with skip = 1 handles both. The roll-up and the per-county
  # files cover disjoint counties (23 + 44 = all 67), so there is no overlap.
  scraped_dir <- "raw/Florida Vaccine Exemption Data (Scraped)"
  fl_cols <- c("census", "county", "state", "total_pop_4_18", "total_exempt_4_18")

  read_scraped <- function(path) {
    readr::read_csv(
      path,
      skip           = 1,
      col_names      = fl_cols,
      col_types      = readr::cols(.default = readr::col_character()),
      show_col_types = FALSE,
      progress       = FALSE
    )
  }

  scraped_files <- list.files(scraped_dir, pattern = "\\.csv$", full.names = TRUE)
  fl_raw <- bind_rows(lapply(scraped_files, read_scraped))

  fl_clean <- fl_raw %>%
    transmute(
      county = str_squish(county),
      total_pop_4_18 = suppressWarnings(
        as.numeric(str_replace_all(str_squish(total_pop_4_18), ",", ""))
      ),
      total_exempt_4_18 = parse_exempt(total_exempt_4_18)
    ) %>%
    filter(!is.na(county), county != "") %>%
    group_by(county) %>%
    summarize(
      total_pop_4_18 = sum(total_pop_4_18, na.rm = TRUE),
      total_exempt_4_18 = sum(total_exempt_4_18, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      time = as.Date("2024-09-01"),
      pct_full_exempt = if_else(
        total_pop_4_18 > 0,
        (total_exempt_4_18 / total_pop_4_18) * 100,
        NA_real_
      )
    )

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fl_fips <- all_fips %>%
    filter(state == "FL", nchar(geography) == 5) %>%
    transmute(
      geography,
      geography_name
    )

  fl_joined <- fl_clean %>%
    left_join(fl_fips, by = c("county" = "geography_name"))

  # Report counties that failed the FIPS join instead of dropping them silently.
  unmatched <- sort(unique(fl_joined$county[is.na(fl_joined$geography)]))
  if (length(unmatched)) {
    warning(length(unmatched), " county name(s) matched no FL FIPS and were dropped: ",
            paste(unmatched, collapse = ", "), call. = FALSE)
  }

  data_out <- fl_joined %>%
    mutate(
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = total_exempt_4_18,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_
    ) %>%
    filter(!is.na(geography)) %>%
    transmute(
      time,
      geography,
      # FDOH labels these "Alachua County"; the standard label is the bare name,
      # as join_county_fips() emits for the states that use it.
      geography_name = sub("\\s+County$", "", county),
      grade,
      N_dtap,
      N_polio,
      N_mmr,
      N_hep_b,
      N_varicella,
      N_personal_exempt,
      N_medical_exempt,
      N_full_exempt,
      pct_dtap,
      pct_polio,
      pct_mmr,
      pct_hep_b,
      pct_varicella,
      pct_personal_exempt,
      pct_medical_exempt,
      pct_full_exempt,
      total_pop_4_18
    )

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "Florida", "standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
