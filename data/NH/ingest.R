# =============================================================================
# NH - Annual School Immunization Report (Percent by County)
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readxl)
library(readr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_end_year <- function(path) {
  m <- str_match(basename(path), "(20\\d{2})-(20\\d{2})")
  if (!is.na(m[1, 1])) return(as.integer(m[1, 3]))
  m2 <- str_match(basename(path), "(20\\d{2})-(\\d{2})")
  if (is.na(m2[1, 1])) return(NA_integer_)
  start_year <- m2[1, 2]
  end_two <- m2[1, 3]
  as.integer(paste0(substr(start_year, 1, 2), end_two))
}

parse_pct_points <- function(x) {
  y <- readr::parse_number(as.character(x))
  if (all(is.na(y))) return(y)
  if (max(y, na.rm = TRUE) <= 1) return(y * 100)
  y
}

parse_file <- function(path) {
  d <- readxl::read_excel(path, sheet = 1, col_names = FALSE)
  idx <- which(d[[1]] == "Percent by County")
  if (length(idx) == 0) return(NULL)

  header_row <- idx[1] + 1
  data_start <- idx[1] + 2
  data_raw <- d[data_start:nrow(d), ]

  # Assign expected column names by position
  if (ncol(data_raw) >= 8) {
    names(data_raw)[1] <- "County"
    names(data_raw)[3] <- "Number Enrolled"
    names(data_raw)[4] <- "Up to Date (1)"
    names(data_raw)[5] <- "Conditionally Enrolled (2)"
    names(data_raw)[6] <- "Religious Exemption (3)"
    names(data_raw)[7] <- "Medical Exemption (4)"
    names(data_raw)[8] <- "Not Up to Date (5)"
  }

  end_year <- parse_end_year(path)
  time <- as.Date(paste0(end_year, "-09-01"))

  data_raw %>%
    filter(!is.na(County)) %>%
    transmute(
      county = str_to_title(str_trim(County)),
      time = time,
      enrollment = readr::parse_number(as.character(`Number Enrolled`)),
      pct_utd = parse_pct_points(`Up to Date (1)`),
      pct_conditional = parse_pct_points(`Conditionally Enrolled (2)`),
      pct_personal_exempt = parse_pct_points(`Religious Exemption (3)`),
      pct_medical_exempt = parse_pct_points(`Medical Exemption (4)`),
      pct_not_utd = parse_pct_points(`Not Up to Date (5)`)
    )
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_files <- list.files("raw", pattern = "\\.xlsx$", full.names = TRUE)
  data_all <- bind_rows(lapply(raw_files, parse_file))

  if (nrow(data_all) > 0) {
    all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
    fips_df <- all_fips %>%
      filter(state == "NH") %>%
      mutate(geography_name = gsub(" County$", "", geography_name))

    state_fips <- fips_df %>%
      filter(nchar(geography) == 2) %>%
      distinct(geography) %>%
      pull(geography)

    data_out <- data_all %>%
      left_join(
        fips_df %>% filter(nchar(geography) == 5),
        by = c("county" = "geography_name")
      ) %>%
      mutate(
        geography = if_else(is.na(geography), state_fips[1], geography),
        geography_name = county,
        grade = "K-12",
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
        pct_full_exempt = NA_real_
      ) %>%
      transmute(
        time, geography, geography_name, grade,
        N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
        N_personal_exempt, N_medical_exempt, N_full_exempt,
        pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
        pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
        enrollment, pct_utd, pct_conditional, pct_not_utd
      )

    dir.create("standard", showWarnings = FALSE)
    vroom::vroom_write(data_out, "standard/data.csv.gz")
  }

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
