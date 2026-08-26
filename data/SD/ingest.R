source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
# =============================================================================
# SD - School Immunization Data Request
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

raw_file <- "raw/Data Request 6.20.25.xlsx"
raw_state <- list(hash = tools::md5sum(raw_file))

script_hash <- as.character(tools::md5sum("ingest.R"))

# Gated on the script as well as the data, like every other state, so an
# edit to the parsing below is actually applied to standard/.
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "SD") %>%
    # Bare county names, matching join_county_fips() in the other states.
    mutate(geography_name = sub(" County$", "", geography_name)) %>%
    select(geography, geography_name, state)

  # str_to_title() capitalizes only the first letter of each word, so SD's two
  # "Mc" counties come out "Mccook"/"Mcpherson" and fail the FIPS join below.
  normalize_county <- function(x) {
    dplyr::recode(x, "Mccook" = "McCook", "Mcpherson" = "McPherson")
  }

  raw <- read_excel(raw_file, sheet = 1)

  data <- raw %>%
    filter(!is.na(All_Schools_County)) %>%
    mutate(
      county = normalize_county(str_to_title(str_trim(All_Schools_County))),
      geography_name = county,
      end_year = as.integer(str_sub(School_Year_School_Year, -4, -1)),
      time = school_year_time_from_end(end_year)
    ) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name"))

  # "Test" is a placeholder entry ("Test Elementary") with no real county.
  # "Wade" matches no SD county; its schools are a mix of Sioux Falls
  # (Minnehaha) buildings and at least one Mitchell (Davison) school, so there
  # is no single county to assign it to without guessing. Both are dropped,
  # reported here instead of silently, like the unmatched check in MI.
  unmatched <- sort(unique(data$geography_name[is.na(data$geography)]))
  if (length(unmatched)) {
    warning(length(unmatched), " county name(s) matched no SD FIPS and were dropped: ",
            paste(unmatched, collapse = ", "), call. = FALSE)
  }
  data <- data %>% filter(!is.na(geography), state == "SD")

  schools <- data %>%
    transmute(
      time,
      geography,
      geography_name,
      type = "school",
      school_name = All_Schools_School_Name,
      N_hep_a = Measures_Table_Total_Hep_A,
      N_hep_b = Measures_Table_Total_Hep_B,
      N_dtap = Measures_Table_Total_Dtap,
      N_tdap = Measures_Table_Total_Tdap,
      N_polio = Measures_Table_Total_Polio,
      N_mmr_k = Measures_Table_Total_MMR_Kindergarten,
      N_varicella = Measures_Table_Total_Varicella,
      N_men_6th = Measures_Table_Total_Men_6th_Grade,
      N_medical_exempt = Measures_Table_Total_Medically_Ex,
      N_religious_exempt = Measures_Table_Total_Religious_Ex
    )

  COUNT_COLS <- c("N_hep_a", "N_hep_b", "N_dtap", "N_tdap",
                  "N_polio", "N_mmr_k", "N_varicella", "N_men_6th",
                  "N_medical_exempt", "N_religious_exempt")

  # County totals, summed across every school in the county for that school
  # year. SD publishes only school-level counts, so the county row is derived
  # rather than reported, the same as MI's building -> county rollup.
  counties <- schools %>%
    group_by(time, geography_name, geography) %>%
    summarize(across(all_of(COUNT_COLS), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
    mutate(type = "county", school_name = NA_character_)

  data_out <- bind_rows(schools, counties) %>%
    select(time, geography_name, geography, type, school_name, everything())

  message(sprintf(
    "SD: %d rows (%d school, %d county), school years %s",
    nrow(data_out), sum(data_out$type == "school"), sum(data_out$type == "county"),
    paste(sort(unique(data_out$time)), collapse = ", ")))

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "South Dakota", "standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
