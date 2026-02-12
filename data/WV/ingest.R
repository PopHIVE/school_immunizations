# =============================================================================
# WV - Exemption Counts by County, School Year, and Grade (Multiple Vaccines)
# =============================================================================

library(dplyr)
library(readxl)
library(stringr)
library(tidyr)
library(purrr)
library(vroom)

if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

raw_file <- "raw/Exempetion Data Request Final 9.2.25 (1).xls"
raw_state <- list(
  hash = tools::md5sum(raw_file),
  script = tools::md5sum("ingest.R"),
  force = "2026-02-05"
)

if (!identical(process$raw_state, raw_state)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "WV") %>%
    select(geography, geography_name, state)

  sheet_map <- tibble::tribble(
    ~sheet, ~vaccine,
    "MMR cnty", "mmr",
    "DTaP cnty", "dtap",
    "Tdap cnty", "tdap",
    "Hib cnty", "hib",
    "Var cnty", "varicella",
    "Men cnty", "menacwy",
    "IPV cnty", "ipv",
    "PCV cnty", "pcv",
    "Hep cnty", "hep"
  )

  parse_sheet <- function(sheet, vaccine) {
    raw <- read_excel(raw_file, sheet = sheet, col_names = FALSE)

    parse_block <- function(start_col, grade_label) {
      header <- raw[2, start_col:(start_col + 9)]
      names(header) <- paste0("c", start_col:(start_col + 9))
      colnames_block <- as.character(header)

      block <- raw[3:nrow(raw), start_col:(start_col + 9)]
      names(block) <- colnames_block

      block <- block %>%
        filter(!is.na(COUNTY)) %>%
        pivot_longer(
          cols = -COUNTY,
          names_to = "school_year",
          values_to = "n_exempt"
        ) %>%
        filter(!is.na(school_year), school_year != "Total") %>%
        mutate(
          grade = grade_label,
          vaccine = vaccine
        )
      block
    }

    k_block <- parse_block(1, "Kindergarten")
    seventh_block <- parse_block(12, "7th Grade")
    twelfth_block <- parse_block(23, "12th Grade")

    bind_rows(k_block, seventh_block, twelfth_block)
  }

  data <- bind_rows(purrr::pmap(sheet_map, parse_sheet)) %>%
    mutate(
      county = str_to_title(str_trim(COUNTY)),
      county = str_replace(county, "^Mcdowell$", "McDowell"),
      geography_name = paste0(county, " County"),
      school_year = str_trim(school_year)
    ) %>%
    filter(!str_detect(county, "^Total"), county != "County") %>%
    filter(str_detect(school_year, "\\d{4}-\\d{4}")) %>%
    mutate(
      end_year = as.integer(str_extract(school_year, "\\d{4}$")),
      n_exempt = as.numeric(n_exempt)
    ) %>%
    filter(!is.na(end_year)) %>%
    mutate(
      time = sprintf("12-31-%d", end_year)
    ) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(!is.na(geography)) %>%
    select(
      time,
      geography,
      geography_name,
      grade,
      vaccine,
      n_exempt
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
