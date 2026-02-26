library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
library(tidyr)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)

  parse_one <- function(path) {
    data_raw <- readxl::read_excel(path, skip = 1, col_names = FALSE) %>%
      setNames(paste0("c", seq_len(ncol(.))))

    year_range <- str_extract(basename(path), "\\d{4}-\\d{2}")
    year_start <- str_extract(year_range, "^\\d{4}")
    time <- format(as.Date(paste0(year_start, "-09-01")), "%m-%d-%Y")

    data_raw %>%
      mutate(
        c1 = as.character(c1),
        c2 = as.character(c2),
        c3 = as.character(c3),
        c4 = as.character(c4),
        c5 = as.character(c5),
        c6 = as.character(c6),
        section_county = if_else(
          str_detect(str_to_upper(str_trim(c1)), "COUNTY$"),
          str_to_upper(str_trim(str_remove(c1, "\\s+COUNTY$"))),
          NA_character_
        )
      ) %>%
      tidyr::fill(section_county, .direction = "down") %>%
      mutate(
        county = case_when(
          str_to_upper(str_trim(c2)) %in% c("HAWAII", "HONOLULU", "KAUAI", "MAUI") ~ str_to_upper(str_trim(c2)),
          str_to_upper(str_trim(c3)) %in% c("HAWAII", "HONOLULU", "KAUAI", "MAUI") ~ str_to_upper(str_trim(c3)),
          TRUE ~ section_county
        ),
        enrollment = readr::parse_number(
          c4,
          na = c("", "NA", "NR", "N/R", "DNR", "Enrollment", "Total Enrollment", "Total\r\nEnrollment")
        ),
        pct_religious = suppressWarnings(readr::parse_number(c5)),
        pct_medical = suppressWarnings(readr::parse_number(c6))
      ) %>%
      filter(
        !is.na(county),
        county %in% c("HAWAII", "HONOLULU", "KAUAI", "MAUI"),
        !is.na(enrollment),
        enrollment > 0,
        str_to_lower(str_trim(c1)) != "school name",
        !str_detect(str_to_upper(c1), "ALL SCHOOLS")
      ) %>%
      mutate(
        pct_religious = if_else(pct_religious > 1, pct_religious / 100, pct_religious),
        pct_medical = if_else(pct_medical > 1, pct_medical / 100, pct_medical),
        N_personal_exempt_school = enrollment * pct_religious,
        N_medical_exempt_school = enrollment * pct_medical,
        time = time
      ) %>%
      select(time, county, enrollment, N_personal_exempt_school, N_medical_exempt_school)
  }

  data_all <- bind_rows(lapply(raw_files, parse_one))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips <- all_fips %>%
    filter(state == "HI", nchar(geography) == 5) %>%
    transmute(
      geography,
      geography_name = str_remove(geography_name, " County$"),
      county = str_to_upper(str_trim(geography_name))
    )

  data_out <- data_all %>%
    group_by(county, time) %>%
    summarize(
      total_enrollment = sum(enrollment, na.rm = TRUE),
      N_personal_exempt = sum(N_personal_exempt_school, na.rm = TRUE),
      N_medical_exempt = sum(N_medical_exempt_school, na.rm = TRUE),
      pct_personal_exempt = if_else(total_enrollment > 0, (N_personal_exempt / total_enrollment) * 100, NA_real_),
      pct_medical_exempt = if_else(total_enrollment > 0, (N_medical_exempt / total_enrollment) * 100, NA_real_),
      .groups = "drop"
    ) %>%
    left_join(county_fips, by = "county") %>%
    mutate(
      geography_name = str_to_title(str_to_lower(geography_name)),
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

  vroom::vroom_write(data_out, "./standard/data.csv.gz", delim = ",")
  vroom::vroom_write(data_out, "./standard/data.csv", delim = ",")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
