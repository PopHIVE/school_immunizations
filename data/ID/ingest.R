library(dcf)
library(dplyr)
library(tidyr)
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
      vax = str_extract(vax_year, "^[^_]+"),
      year_range = str_extract(vax_year, "\\d{2}-\\d{2}$"),
      year_end2 = str_extract(year_range, "\\d{2}$"),
      year_end = paste0("20", year_end2),
      time = as.Date(paste0(year_end, "-09-01")),
      value = readr::parse_number(as.character(value))
    ) %>%
    filter(!is.na(time), !is.na(vax))
  
  vax_map <- c(
    "DTaP" = "dtap",
    "Polio" = "polio",
    "MMR" = "mmr",
    "Varicella" = "varicella",
    "Hepatitis_B" = "hep_b"
  )
  
  data_wide <- data_long %>%
    mutate(vax_key = vax_map[vax]) %>%
    filter(!is.na(vax_key)) %>%
    select(county, time, vax_key, value) %>%
    pivot_wider(names_from = vax_key, values_from = value)
  
  for (col in c("dtap", "polio", "mmr", "hep_b", "varicella")) {
    if (!col %in% names(data_wide)) data_wide[[col]] <- NA_real_
  }
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "ID") %>%
    mutate(geography_name = gsub(" County", "", geography_name))
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  data_out <- data_wide %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = c("county" = "geography_name")
    ) %>%
    mutate(
      geography = if_else(is.na(geography), state_fips[1], geography),
      geography_name = county,
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      pct_dtap = dtap,
      pct_polio = polio,
      pct_mmr = mmr,
      pct_hep_b = hep_b,
      pct_varicella = varicella,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_,
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
