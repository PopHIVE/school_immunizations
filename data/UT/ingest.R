source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
# =============================================================================
# UT - Vaccine Exemption Rates by School District (2018-2023)
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readxl)
library(readr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

# The school-year END year comes from school_year_end_from_label() in
# resources/school_year.R. The local copy this replaces was not vectorised -- it
# indexed the str_match() matrix at [1, 3] and returned that one value for every
# row, so although `Year` runs 2018-2019 through 2022-2023, all 615 rows came out
# stamped with the first row's year and Utah published one school year of five.

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_path <- "./raw/Utah Vaccine Exemption.xlsx"
  sheets <- readxl::excel_sheets(raw_path)

  data_all <- bind_rows(lapply(sheets, function(sh) {
    grade <- case_when(
      str_detect(tolower(sh), "kindergarten") ~ "Kindergarten",
      str_detect(tolower(sh), "7th") ~ "7th grade",
      str_detect(tolower(sh), "k-12") ~ "K-12",
      TRUE ~ sh
    )

    d <- readxl::read_excel(raw_path, sheet = sh)

    d %>%
      transmute(
        health_district = str_trim(`Health District`),
        school_district = str_trim(`School Distric`),
        year = `Year`,
        end_year = school_year_end_from_label(`Year`),
        time = as.Date(school_year_time_from_end(end_year)),
        pct_full_exempt = readr::parse_number(as.character(`Total Exemption Rate (%)`)),
        grade = grade
      )
  }))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips <- all_fips %>%
    filter(state == "UT", nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)

  if (any(is.na(data_all$time))) {
    stop("UT: ", sum(is.na(data_all$time)), " row(s) have a `Year` that does ",
         "not parse as a school year: ",
         paste(unique(data_all$year[is.na(data_all$time)]), collapse = ", "))
  }

  # Utah publishes by health and school district, not by county, and its school
  # districts do not nest in counties -- so these rows carry the state FIPS and
  # keep the district labels that identify them. They are sub-state rows, not
  # statewide totals.
  data_out <- data_all %>%
    mutate(
      geography = state_fips[1],
      geography_name = "Utah"
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      pct_full_exempt,
      health_district, school_district
    )

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "Utah", "standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
