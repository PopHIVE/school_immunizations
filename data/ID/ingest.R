library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  raw_path <- "./raw/Yale School Exemption Data Request (2) (1).xlsx"
  header_rows <- readxl::read_excel(raw_path, sheet = "Data", n_max = 2, col_names = FALSE)
  h1 <- as.character(header_rows[1, ])
  h2 <- as.character(header_rows[2, ])
  h1[h1 == "NA"] <- NA_character_
  h2[h2 == "NA"] <- NA_character_
  
  vaccine_fill <- character(length(h1))
  current_vax <- NA_character_
  for (i in seq_along(h1)) {
    if (!is.na(h1[i]) && h1[i] != "") current_vax <- h1[i]
    vaccine_fill[i] <- current_vax
  }
  
  col_names <- vapply(seq_along(h1), function(i) {
    if (!is.na(h1[i]) && h1[i] == "County") return("County")
    vax <- vaccine_fill[i]
    yr <- h2[i]
    if (is.na(vax) || is.na(yr) || vax == "" || yr == "") return(paste0("skip", i))
    nm <- paste0(vax, "_", yr)
    nm <- str_replace_all(nm, "\\s+", "_")
    nm <- str_replace_all(nm, "[^A-Za-z0-9_\\-]+", "_")
    nm <- str_replace_all(nm, "_+", "_")
    str_replace_all(nm, "_$", "")
  }, character(1))
  
  data_raw <- readxl::read_excel(
    raw_path,
    sheet = "Data",
    skip = 2,
    col_names = col_names
  ) %>%
    rename(county = County) %>%
    mutate(across(-county, as.character))
  
  data_long <- data_raw %>%
    pivot_longer(
      cols = -county,
      names_to = "vax_year",
      values_to = "value"
    ) %>%
    mutate(
      # Everything before the trailing "NN-NN" year range is the vaccine name.
      # This used to be str_extract(vax_year, "^[^_]+"), which stops at the
      # first underscore: "Hepatitis_B_23-24" yielded "Hepatitis", matched
      # nothing in vax_map, and both hepatitis series were dropped -- which is
      # why pct_hep_b was entirely missing for Idaho.
      vax = str_replace(vax_year, "_\\d{2}-\\d{2}$", ""),
      year_range = str_extract(vax_year, "\\d{2}-\\d{2}$"),
      year_end2 = str_extract(year_range, "\\d{2}$"),
      year_end = paste0("20", year_end2),
      time = as.Date(school_year_time_from_end(year_end)),
      value = parse_rate(value, from = "rate")
    ) %>%
    filter(!is.na(time), !is.na(vax))

  # DHW's workbook is an exemption request ("Yale School Exemption Data
  # Request"): every cell is the share of students EXEMPT from that series, not
  # the share vaccinated. The columns are therefore named as exemptions, and the
  # coverage columns are left missing -- the source publishes no coverage, and
  # 1 - exemption is not coverage (a student can be unvaccinated without an
  # exemption, and an exempt student may still have had the vaccine).
  vax_map <- c(
    "DTaP" = "dtap",
    "Polio" = "polio",
    "MMR" = "mmr",
    "Varicella" = "varicella",
    "Hepatitis_A" = "hep_a",
    "Hepatitis_B" = "hep_b",
    # DHW's heading for the meningococcal requirement (IDAPA 16.02.15 requires
    # MenACWY at 7th and 12th grade).
    "Meningitis" = "menacwy"
  )

  unmapped <- setdiff(unique(data_long$vax), names(vax_map))
  if (length(unmapped)) {
    stop("ID: unmapped vaccine column(s): ", paste(unmapped, collapse = ", "),
         call. = FALSE)
  }

  data_wide <- data_long %>%
    mutate(vax_key = paste0("pct_", vax_map[vax], "_exempt")) %>%
    select(county, time, vax_key, value) %>%
    pivot_wider(names_from = vax_key, values_from = value)

  exempt_cols <- paste0("pct_", unname(vax_map), "_exempt")
  for (col in exempt_cols) {
    if (!col %in% names(data_wide)) data_wide[[col]] <- NA_real_
  }

  data_out <- data_wide %>%
    join_county_fips("ID") %>%
    mutate(
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      # Coverage: not published by this source (see above).
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      # DHW reports the exemption total per series, not split by reason.
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_,
      pct_full_exempt = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      pct_dtap_exempt, pct_polio_exempt, pct_mmr_exempt,
      pct_hep_a_exempt, pct_hep_b_exempt, pct_varicella_exempt,
      pct_menacwy_exempt
    )
  
  write_standard(data_out, "Idaho", "./standard/data.csv.gz", from = "rate")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
