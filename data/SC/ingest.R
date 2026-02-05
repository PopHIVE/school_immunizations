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
  
  raw_path <- "./raw/SC_2019-23.csv"
  header_lines <- readr::read_lines(raw_path, n_max = 3)
  
  header_year <- readr::read_csv(I(header_lines[2]), col_names = FALSE, show_col_types = FALSE)
  header_metric <- readr::read_csv(I(header_lines[3]), col_names = FALSE, show_col_types = FALSE)
  h2 <- as.character(header_year[1, ])
  h3 <- as.character(header_metric[1, ])
  h2[h2 == "NA"] <- NA_character_
  year_fill <- character(length(h2))
  current_year <- NA_character_
  for (i in seq_along(h2)) {
    if (!is.na(h2[i]) && h2[i] != "") current_year <- h2[i]
    year_fill[i] <- current_year
  }
  
  col_names <- vapply(seq_along(h2), function(i) {
    a <- h2[i]
    b <- h3[i]
    if (!is.na(a) && a %in% c("State", "County")) return(a)
    if (is.na(a) || a == "") a <- year_fill[i]
    if (is.na(a) || a == "") return(paste0("col", i))
    metric_key <- if (!is.na(b) && b != "") {
      dplyr::case_when(
        str_detect(b, "Enrolled") ~ "Enrolled",
        str_detect(b, "%|Percent") ~ "ExemptPercent",
        str_detect(b, "#|Number") ~ "ExemptCount",
        TRUE ~ b
      )
    } else {
      "value"
    }
    nm <- paste0(a, "_", metric_key)
    nm <- str_replace_all(nm, "\\s+", "_")
    nm <- str_replace_all(nm, "[^A-Za-z0-9_\\-]+", "_")
    nm <- str_replace_all(nm, "_+", "_")
    str_replace_all(nm, "_$", "")
  }, character(1))
  
  data_raw <- readr::read_csv(
    raw_path,
    skip = 3,
    col_names = col_names,
    show_col_types = FALSE
  ) %>%
    mutate(State = if_else(State == "", NA_character_, State)) %>%
    tidyr::fill(State, .direction = "down") %>%
    rename(state = State, county = County) %>%
    mutate(across(-c(state, county), as.character))
  
  data_long <- data_raw %>%
    pivot_longer(
      cols = -c(state, county),
      names_to = "year_metric",
      values_to = "value"
    ) %>%
    mutate(
      year = str_extract(year_metric, "^\\d{4}-\\d{4}"),
      metric = str_remove(year_metric, "^\\d{4}-\\d{4}_"),
      value = readr::parse_number(as.character(value))
    ) %>%
    filter(!is.na(year))
  
  data_wide <- data_long %>%
    select(state, county, year, metric, value) %>%
    pivot_wider(names_from = metric, values_from = value)
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "SC") %>%
    mutate(geography_name = gsub(" County", "", geography_name))
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  data_out <- data_wide %>%
    mutate(
      county = str_trim(county),
      county = if_else(county %in% c("State Totals", "State Total", "Total"), "Total", county)
    ) %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = c("county" = "geography_name")
    ) %>%
    mutate(
      geography = if_else(county == "Total", state_fips[1], geography),
      geography_name = county,
      yearpart = sub(".*-", "", year),
      time = as.Date(paste0(yearpart, "-09-01")),
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = ExemptCount,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_personal_exempt = ExemptPercent,
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
