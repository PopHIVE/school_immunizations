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
  
  raw_files <- list.files("./raw", pattern = "\\.csv$", full.names = TRUE)
  
  parse_one_file <- function(path) {
    header_rows <- readr::read_csv(
      path,
      skip = 1,
      n_max = 2,
      col_names = FALSE,
      show_col_types = FALSE
    )
    h2 <- as.character(header_rows[1, ])
    h3 <- as.character(header_rows[2, ])
    h2[h2 == "NA"] <- NA_character_
    h3[h3 == "NA"] <- NA_character_
    
    grade_fill <- character(length(h2))
    current_grade <- NA_character_
    for (i in seq_along(h2)) {
      if (!is.na(h2[i]) && h2[i] != "") current_grade <- h2[i]
      grade_fill[i] <- current_grade
    }
    
    col_names <- vapply(seq_along(h2), function(i) {
      if (!is.na(h2[i]) && h2[i] == "County") return("County")
      grade <- grade_fill[i]
      metric_raw <- h3[i]
      metric_key <- if (is.na(metric_raw) || metric_raw == "") {
        paste0("skip", i)
      } else {
        dplyr::case_when(
          str_detect(metric_raw, "Total Number") ~ "Total",
          str_detect(metric_raw, "Number") ~ "ExemptCount",
          str_detect(metric_raw, "Percent") ~ "ExemptPercent",
          TRUE ~ metric_raw
        )
      }
      nm <- paste0(grade, "_", metric_key)
      nm <- str_replace_all(nm, "\\s+", "_")
      nm <- str_replace_all(nm, "[^A-Za-z0-9_\\-]+", "_")
      nm <- str_replace_all(nm, "_+", "_")
      str_replace_all(nm, "_$", "")
    }, character(1))
    
    data_raw <- readr::read_csv(
      path,
      skip = 3,
      col_names = col_names,
      show_col_types = FALSE
    ) %>%
      rename(county = County) %>%
      mutate(across(-county, as.character))
    
    year_match <- str_match(basename(path), "NJ_(\\d{2})-(\\d{2})\\.csv")
    year_start <- paste0("20", year_match[2])
    year_end <- paste0("20", year_match[3])
    year <- paste0(year_start, "-", year_end)
    
    data_long <- data_raw %>%
      pivot_longer(
        cols = -county,
        names_to = "grade_metric",
        values_to = "value"
      ) %>%
      mutate(
        grade = str_match(grade_metric, "^(.*)_(ExemptCount|ExemptPercent)$")[, 2],
        metric = str_match(grade_metric, "^(.*)_(ExemptCount|ExemptPercent)$")[, 3],
        value = readr::parse_number(as.character(value)),
        year = year
      ) %>%
      filter(!is.na(metric)) %>%
      pivot_wider(names_from = metric, values_from = value)
    
    data_long
  }
  
  data_all <- bind_rows(lapply(raw_files, parse_one_file)) %>%
    group_by(county, grade, year) %>%
    summarize(
      ExemptCount = if (all(is.na(ExemptCount))) NA_real_ else max(ExemptCount, na.rm = TRUE),
      ExemptPercent = if (all(is.na(ExemptPercent))) NA_real_ else max(ExemptPercent, na.rm = TRUE),
      .groups = "drop"
    )
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "NJ") %>%
    mutate(geography_name = gsub(" County", "", geography_name))
  
  state_fips <- fips_df %>%
    filter(nchar(geography) == 2) %>%
    distinct(geography) %>%
    pull(geography)
  
  normalize_grade <- function(x) {
    x <- str_to_lower(x)
    case_when(
      str_detect(x, "pre") ~ "Pre-K",
      str_detect(x, "kindergarten") ~ "Kindergarten",
      str_detect(x, "first") ~ "1st grade",
      str_detect(x, "sixth") ~ "6th grade",
      str_detect(x, "transfer") ~ "Transfer",
      str_detect(x, "total") ~ "Overall",
      TRUE ~ str_to_title(x)
    )
  }
  
  data_out <- data_all %>%
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
      yearpart = str_extract(year, "\\d{4}$"),
      time = as.Date(paste0(yearpart, "-09-01")),
      grade = normalize_grade(grade),
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = ExemptCount,
      N_full_exempt = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = ExemptPercent,
      pct_full_exempt = NA_real_
    ) %>%
    filter(!is.na(time)) %>%
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
