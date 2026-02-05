library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)
  
  parse_one <- function(path) {
    data_raw <- readxl::read_excel(path, skip = 1)
    year_range <- str_extract(basename(path), "\\d{4}-\\d{2}")
    year_start <- str_extract(year_range, "^\\d{4}")
    year_end2 <- str_extract(year_range, "\\d{2}$")
    year_end <- paste0(substr(year_start, 1, 2), year_end2)
    time <- as.Date(paste0(year_end, "-09-01"))
    
    data_raw %>%
      rename(
        school_name = `School Name`,
        county = County,
        enrollment = Enrollment,
        pct_religious = `Religious Exemptions`,
        pct_medical = `Medical Exemptions`
      ) %>%
      filter(!is.na(county)) %>%
      mutate(
        county = str_trim(county),
        enrollment = readr::parse_number(as.character(enrollment)),
        pct_religious = as.numeric(pct_religious),
        pct_medical = as.numeric(pct_medical),
        time = time
      )
  }
  
  data_all <- bind_rows(lapply(raw_files, parse_one))
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "HI") %>%
    mutate(geography_name = gsub(" County", "", geography_name))
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  data_out <- data_all %>%
    group_by(county, time) %>%
    summarize(
      total_enroll = sum(enrollment, na.rm = TRUE),
      N_personal_exempt = sum(enrollment * pct_religious, na.rm = TRUE),
      N_medical_exempt = sum(enrollment * pct_medical, na.rm = TRUE),
      pct_personal_exempt = ifelse(total_enroll > 0,
                                   N_personal_exempt / total_enroll,
                                   NA_real_),
      pct_medical_exempt = ifelse(total_enroll > 0,
                                  N_medical_exempt / total_enroll,
                                  NA_real_),
      .groups = "drop"
    ) %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = c("county" = "geography_name")
    ) %>%
    mutate(
      geography = if_else(is.na(geography), state_fips[1], geography),
      geography_name = if_else(is.na(geography_name), "Hawaii", county),
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_full_exempt = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_full_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt
    )
  
  vroom::vroom_write(data_out, "./standard/data.csv.gz")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
