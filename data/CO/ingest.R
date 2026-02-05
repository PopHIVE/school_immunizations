library(dcf)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", "csv", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  raw_path <- "./raw/CDPHE_Colorado_School_and_Child_Care_Immunization_County_Data_.csv"
  data_raw <- readr::read_csv(raw_path, show_col_types = FALSE) %>%
    select(-OBJECTID)
  
  data_exempt <- data_raw %>%
    filter(Metric %in% c("MedicalExemptions", "NonMedicalExemptions")) %>%
    mutate(
      year_end = str_extract(Year_, "\\d{4}$"),
      time = as.Date(paste0(year_end, "-09-01")),
      grade = Survey_Type,
      vaccine_key = tolower(Vaccine),
      vaccine_key = gsub("[^a-z0-9]+", "_", vaccine_key),
      vaccine_key = gsub("_+", "_", vaccine_key),
      metric_key = case_when(
        Metric == "MedicalExemptions" ~ "medical_exempt",
        Metric == "NonMedicalExemptions" ~ "nonmedical_exempt",
        TRUE ~ "exempt"
      ),
      value = as.numeric(Value_Percent)
    ) %>%
    filter(!is.na(time), !is.na(vaccine_key)) %>%
    mutate(col_name = paste0("pct_", vaccine_key, "_", metric_key)) %>%
    select(County, time, grade, col_name, value) %>%
    pivot_wider(
      names_from = col_name,
      values_from = value,
      values_fn = list(value = function(x) max(x, na.rm = TRUE))
    )
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "CO") %>%
    mutate(geography_name = gsub(" County", "", geography_name))
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  data_out <- data_exempt %>%
    rename(county = County) %>%
    mutate(
      county = if_else(county %in% c("State Total", "State Totals", "Total"), "Total", county)
    ) %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = c("county" = "geography_name")
    ) %>%
    mutate(
      geography = if_else(county == "Total", state_fips[1], geography),
      geography_name = county,
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_,
      pct_full_exempt = NA_real_
    )
  
  vroom::vroom_write(data_out, "./standard/data.csv.gz")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
