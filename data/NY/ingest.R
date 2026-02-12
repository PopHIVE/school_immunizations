# =============================================================================
# NY - School Immunization Survey (School-Level)
# =============================================================================

library(dplyr)
library(readxl)
library(stringr)
library(vroom)

if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

raw_file <- "raw/New York School_Immunization_Survey___Beginning_2019-20_School_Year_20250426.xlsx"
raw_state <- list(hash = tools::md5sum(raw_file))

if (!identical(process$raw_state, raw_state)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "NY") %>%
    select(geography, geography_name, state)

  raw <- read_excel(raw_file, sheet = 1)

  percent_cols <- names(raw)[grepl("^Percent ", names(raw))]

  data <- raw %>%
    mutate(
      county = str_to_title(str_trim(County)),
      geography_name = paste0(county, " County"),
      end_year = as.integer(str_sub(`Report Period`, -4, -1)),
      time = format(as.Date(paste0(end_year, "-12-31")), "%m-%d-%Y")
    ) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(state == "NY")

  data <- data %>%
    mutate(across(all_of(percent_cols), ~ suppressWarnings(as.numeric(.x)))) %>%
    mutate(
      across(
        all_of(percent_cols),
        ~ if_else(!is.na(.x) & .x <= 1.5, .x * 100, .x)
      )
    ) %>%
    mutate(
      across(
        all_of(percent_cols),
        ~ if_else(.x > 100, NA_real_, .x)
      )
    )

  data_out <- data %>%
    rename_with(
      ~ str_replace_all(
        tolower(.x),
        c(
          "percent " = "pct_",
          " " = "_",
          "/" = "_"
        )
      ),
      all_of(percent_cols)
    ) %>%
    mutate(school_id = as.character(`School ID`)) %>%
    select(
      time,
      geography,
      geography_name,
      school_id,
      school_name = `School Name`,
      district = District,
      school_type = Type,
      starts_with("pct_")
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
