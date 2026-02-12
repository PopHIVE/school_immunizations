# =============================================================================
# ND - School Immunization Dashboard (Aggregated to County)
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

raw_file <- "raw/Dashboard data for requests.xlsx"
raw_state <- list(hash = tools::md5sum(raw_file))

if (!identical(process$raw_state, raw_state)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "ND") %>%
    select(geography, geography_name, state)

  raw <- read_excel(raw_file, sheet = 1)

  percent_cols <- names(raw)[grepl("^%", names(raw))]

  data <- raw %>%
    mutate(
      county = str_to_title(str_trim(County)),
      geography_name = paste0(county, " County"),
      grade = case_when(
        tolower(Grade) == "k" ~ "Kindergarten",
        TRUE ~ Grade
      ),
      end_year = as.integer(str_sub(`School Year`, -4, -1)),
      time = format(as.Date(paste0(end_year, "-12-31")), "%m-%d-%Y")
    ) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(state == "ND")

  data_agg <- data %>%
    group_by(geography, geography_name, time, grade) %>%
    summarize(
      n_enrolled = sum(Enrolled, na.rm = TRUE),
      across(
        all_of(percent_cols),
        ~ if (all(is.na(.x))) NA_real_ else weighted.mean(.x, Enrolled, na.rm = TRUE)
      ),
      .groups = "drop"
    )

  data_out <- data_agg %>%
    rename_with(
      ~ str_replace_all(
        tolower(.x),
        c(
          "% " = "pct_",
          "%no record" = "pct_no_record",
          "%pbe" = "pct_pbe",
          "%me" = "pct_medical_exempt",
          "%re" = "pct_religious_exempt",
          "%utd " = "pct_utd_",
          "%utd" = "pct_utd_",
          " " = "_",
          "/" = "_"
        )
      ),
      all_of(percent_cols)
    ) %>%
    select(
      time,
      geography,
      geography_name,
      grade,
      n_enrolled,
      starts_with("pct_")
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
