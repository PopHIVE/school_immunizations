library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_num <- function(x) {
  x <- str_squish(as.character(x))
  is_lt <- str_detect(x, "^<")
  x <- str_remove(x, "^<\\s*")
  x <- str_replace_all(x, ",", "")
  out <- suppressWarnings(as.numeric(x))
  if_else(is_lt & !is.na(out), out / 2, out)
}

as_pct_points <- function(x) {
  if (all(is.na(x))) return(x)
  if (max(x, na.rm = TRUE) <= 1) return(x * 100)
  x
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  ri_raw <- readxl::read_excel(
    "raw/RI_RE rates_Yale_Aug 2025 (1).xlsx",
    sheet = "Sheet1",
    col_names = FALSE
  )

  colnames(ri_raw)[1:9] <- c(
    "state_abbr", "school_year", "grade_raw",
    "pct_mmr", "pct_dtap", "pct_polio", "pct_varicella", "pct_mcv", "pct_hep_b"
  )

  ri_clean <- ri_raw %>%
    slice(-(1:2)) %>%
    transmute(
      state_abbr = str_squish(state_abbr),
      school_year = str_squish(school_year),
      grade = case_when(
        str_to_lower(str_squish(grade_raw)) == "k" ~ "Kindergarten",
        TRUE ~ str_squish(grade_raw)
      ),
      start_year = str_extract(school_year, "^\\d{4}"),
      time = suppressWarnings(as.Date(paste0(start_year, "-09-01"), format = "%Y-%m-%d")),
      pct_mmr = as_pct_points(parse_num(pct_mmr)),
      pct_dtap = as_pct_points(parse_num(pct_dtap)),
      pct_polio = as_pct_points(parse_num(pct_polio)),
      pct_varicella = as_pct_points(parse_num(pct_varicella)),
      pct_hep_b = as_pct_points(parse_num(pct_hep_b))
    ) %>%
    filter(!is.na(state_abbr), state_abbr != "", !is.na(time))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  state_fips <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state, geography_name)

  data_out <- ri_clean %>%
    left_join(state_fips, by = c("state_abbr" = "state")) %>%
    mutate(
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_,
      pct_full_exempt = NA_real_
    ) %>%
    filter(!is.na(geography)) %>%
    transmute(
      time,
      geography,
      geography_name,
      grade,
      N_dtap,
      N_polio,
      N_mmr,
      N_hep_b,
      N_varicella,
      N_personal_exempt,
      N_medical_exempt,
      N_full_exempt,
      pct_dtap,
      pct_polio,
      pct_mmr,
      pct_hep_b,
      pct_varicella,
      pct_personal_exempt,
      pct_medical_exempt,
      pct_full_exempt
    )

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
