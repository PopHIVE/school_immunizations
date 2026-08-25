library(dcf)
library(dplyr)
library(readr)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/county_fips.R")
source("../../resources/school_year.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  raw_files <- list.files("./raw", pattern = "\\.csv$", full.names = TRUE)
  
  # NMDOH publishes these by CALENDAR year, not by school year: each file's own
  # title reads "NM Student Vaccine Exemptions by County, Age <=18, Calendar Year
  # 2012", and NM_12.csv is that calendar year.
  #
  # The standard `time` is the September a school year started, so the calendar
  # year is taken as the year the school year ENDED: calendar year 2012 is dated
  # 2011-09-01, the school year 2011-12 that runs through the first half of it.
  # This is a mapping decision, not something the source states -- a calendar
  # year overlaps the tail of one school year and the head of the next -- so it
  # is recorded here rather than left implicit. Every NM series shifts by a year
  # if it is ever revisited.
  data_all <- bind_rows(lapply(raw_files, function(path) {
    year_match <- str_match(basename(path), "NM_(\\d{2})\\.csv")
    calendar_year <- if (!is.na(year_match[2])) paste0("20", year_match[2])
                     else NA_character_
    time <- if (!is.na(calendar_year))
      as.Date(school_year_time_from_end(calendar_year)) else as.Date(NA)


    data_raw <- readr::read_csv(path, skip = 1, show_col_types = FALSE)
    names(data_raw) <- c("county", "exempt_count", "population", "exempt_per_1000", "pct_full_exempt")
    
    data_raw %>%
      transmute(
        county = county,
        N_full_exempt = readr::parse_number(as.character(exempt_count)),
        total_pop_18 = readr::parse_number(as.character(population)),
        pct_full_exempt = readr::parse_number(as.character(pct_full_exempt)),
        time = time
      ) %>%
      filter(!is.na(county), !is.na(time))
  }))
  
  # Each sheet ends with the "* Small numbers..." footnote in the county
  # column; it used to be kept and filed as a statewide row.
  data_out <- data_all %>%
    mutate(county = gsub("\\*$", "", county)) %>%
    join_county_fips(
      "NM",
      statewide = "Total",
      drop = "^\\*|small numbers may result"
    ) %>%
    mutate(
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      total_pop_18,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt
    )
  
  write_standard(data_out, "New Mexico", "./standard/data.csv.gz", from = "percent")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
