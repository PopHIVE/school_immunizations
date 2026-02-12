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

if (!identical(process$raw_state, raw_state)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "SD") %>%
    select(geography, geography_name, state)

  raw <- read_excel(raw_file, sheet = 1)

  data <- raw %>%
    filter(!is.na(All_Schools_County)) %>%
    mutate(
      county = str_to_title(str_trim(All_Schools_County)),
      geography_name = paste0(county, " County"),
      end_year = as.integer(str_sub(School_Year_School_Year, -4, -1)),
      time = format(as.Date(paste0(end_year, "-12-31")), "%m-%d-%Y")
    ) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(state == "SD")

  data_out <- data %>%
    transmute(
      time,
      geography,
      geography_name,
      school_name = All_Schools_School_Name,
      n_total_audited = All_Schools_Total_audited,
      n_hep_a = Measures_Table_Total_Hep_A,
      n_hep_b = Measures_Table_Total_Hep_B,
      n_dtap = Measures_Table_Total_Dtap,
      n_tdap = Measures_Table_Total_Tdap,
      n_polio = Measures_Table_Total_Polio,
      n_mmr_k = Measures_Table_Total_MMR_Kindergarten,
      n_varicella = Measures_Table_Total_Varicella,
      n_men_6th = Measures_Table_Total_Men_6th_Grade,
      n_medical_exempt = Measures_Table_Total_Medically_Ex,
      n_religious_exempt = Measures_Table_Total_Religious_Ex
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
