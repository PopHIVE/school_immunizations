source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
# =============================================================================
# IA - K-12 Medical and Religious Exemptions by County
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readr)
library(purrr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_end_year <- function(path) {
  m <- str_match(basename(path), "(20\\d{2})-(\\d{2})")
  if (is.na(m[1, 1])) return(NA_integer_)
  start_year <- m[1, 2]
  end_two <- m[1, 3]
  as.integer(paste0(substr(start_year, 1, 2), end_two))
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  read_exempt <- function(path, type) {
    end_year <- parse_end_year(path)
    time <- as.Date(school_year_time_from_end(end_year))

    d <- read_delim(
      path,
      delim = "\t",
      locale = locale(encoding = "UTF-16LE"),
      show_col_types = FALSE
    )

    d %>%
      transmute(
        county = str_to_title(str_trim(County)),
        time = time,
        total_enrolled = readr::parse_number(as.character(`Total Enrolled`)),
        n_exempt = readr::parse_number(as.character(`Number of Certificates`)),
        # Computed from the counts the file already carries, not parsed from
        # `Percent of Certificates`.
        #
        # HHS medical-exemption rates are all below 1 percent, so the old
        # column-global `if (max(y) <= 1) y * 100` test fired on those files and
        # multiplied every value by 100: Dickinson County 2020-21 has 27 medical
        # certificates against 2,707 enrolled -- a true 1.0% -- and shipped as
        # 100%. That is what pushed pct_full_exempt above 100.
        #
        # A rate computed from numerator and denominator has no scale to get
        # wrong, so this needs no assumption about how the file is formatted.
        pct_exempt = rate_from_counts(n_exempt, total_enrolled),
        type = type
      )
  }

  med_files <- list.files(
    "raw/K-12/Medical Exemption",
    pattern = "\\.csv$",
    full.names = TRUE
  )
  rel_files <- list.files(
    "raw/K-12/Religious Exemption",
    pattern = "\\.csv$",
    full.names = TRUE
  )

  med_files <- med_files[!str_detect(basename(med_files), "^~\\$")]
  rel_files <- rel_files[!str_detect(basename(rel_files), "^~\\$")]

  data_med <- bind_rows(lapply(med_files, read_exempt, type = "medical"))
  data_rel <- bind_rows(lapply(rel_files, read_exempt, type = "religious"))

  data_wide <- full_join(
    data_med %>% rename(n_medical_exempt = n_exempt,
                        pct_medical_exempt = pct_exempt),
    data_rel %>% rename(n_personal_exempt = n_exempt,
                        pct_personal_exempt = pct_exempt),
    by = c("county", "time", "total_enrolled")
  ) %>%
    mutate(
      N_full_exempt = if_else(
        is.na(n_medical_exempt) & is.na(n_personal_exempt),
        NA_real_,
        coalesce(n_medical_exempt, 0) + coalesce(n_personal_exempt, 0)
      ),
      # Taken straight from the combined count over the same denominator, rather
      # than by adding the two component rates: the medical and religious files
      # are joined on total_enrolled, so a year where the two disagree on a
      # county's enrolment would otherwise sum rates with different bases.
      pct_full_exempt = rate_from_counts(N_full_exempt, total_enrolled)
    )

  # The source title-cases county names, which mangles O'Brien to "O'brien".
  data_out <- data_wide %>%
    join_county_fips("IA") %>%
    mutate(
      grade = "K-12",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt = n_personal_exempt,
      N_medical_exempt = n_medical_exempt,
      N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt,
      pct_medical_exempt,
      pct_full_exempt,
      total_enrolled
    )

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "Iowa", "standard/data.csv.gz", from = "rate")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
