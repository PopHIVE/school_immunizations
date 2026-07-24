library(dcf)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(vroom)
source("../../resources/add_state_column.R")

# ---- Download from open-data API ----
# Source: CDPHE ArcGIS Open Data — Colorado School and Child Care Immunization
#   County Data (also surfaced on data.colorado.gov). Pulled directly so the
#   source self-updates; the manual CSV download is no longer required.
api_url <- paste0(
  "https://opendata.arcgis.com/api/v3/datasets/",
  "69a2917e89a0456fbe3f04dfb6767621_1/downloads/data?format=csv&spatialRefId=4326"
)
dir.create("raw", showWarnings = FALSE)
raw_path <- "./raw/CDPHE_Colorado_School_and_Child_Care_Immunization_County_Data_.csv"
try(
  download.file(api_url, raw_path, mode = "wb", quiet = TRUE),
  silent = TRUE
)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", "csv", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  data_raw <- readr::read_csv(raw_path, show_col_types = FALSE)
  # The live API names this column "Year"; older manual exports used "Year_".
  if ("Year" %in% names(data_raw) && !("Year_" %in% names(data_raw))) {
    data_raw <- dplyr::rename(data_raw, Year_ = Year)
  }
  data_raw <- data_raw %>%
    select(-OBJECTID)
  
  # The dataset's Metric labels changed over time: older manual exports used
  # "MedicalExemptions"/"NonMedicalExemptions"; the live API uses
  # "Medical Exemption"/"Nonmedical Exemption". Accept both.
  data_exempt <- data_raw %>%
    filter(Metric %in% c(
      "MedicalExemptions", "NonMedicalExemptions",
      "Medical Exemption", "Nonmedical Exemption"
    )) %>%
    mutate(
      year_end = str_extract(Year_, "\\d{4}$"),
      time = as.Date(paste0(year_end, "-09-01")),
      grade = Survey_Type,
      vaccine_key = tolower(Vaccine),
      vaccine_key = gsub("[^a-z0-9]+", "_", vaccine_key),
      vaccine_key = gsub("_+", "_", vaccine_key),
      metric_key = case_when(
        Metric %in% c("MedicalExemptions", "Medical Exemption") ~ "medical_exempt",
        Metric %in% c("NonMedicalExemptions", "Nonmedical Exemption") ~ "nonmedical_exempt",
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
      # The live API returns county names in uppercase (e.g. "EL PASO"); the
      # FIPS lookup uses title case (e.g. "El Paso"). Normalize so the join works.
      county = str_to_title(county),
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
  
  vroom::vroom_write(add_state_column(data_out, "Colorado"), "./standard/data.csv.gz")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
