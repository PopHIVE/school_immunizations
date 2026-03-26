library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)

raw_state <- as.list(tools::md5sum(list.files(
  "Raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  
  raw_path <- "./Raw/Massachusetts Vaccine Exemption.xlsx"
  sheets <- readxl::excel_sheets(raw_path)
  kg_sheet <- sheets[str_trim(sheets) == "Kindergarten 20-25 by County"][1]
  g7_sheet <- sheets[str_detect(str_trim(sheets), "^Grade 7 20-25 by County")][1]
  
  if (is.na(kg_sheet) || is.na(g7_sheet)) {
    stop("Required MA county sheets were not found in raw workbook.")
  }
  
  parse_start_year <- function(year_str) {
    m <- str_match(as.character(year_str), "^(\\d{4})-(?:\\d{2}|\\d{4})$")
    as.integer(m[, 2])
  }
  
  get_col <- function(df, choices) {
    found <- choices[choices %in% names(df)]
    if (length(found) == 0) {
      return(rep(NA, nrow(df)))
    }
    df[[found[1]]]
  }
  
  parse_pct <- function(x) {
    readr::parse_number(as.character(x))
  }
  
  load_county_sheet <- function(sh, grade_label) {
    excluded_rows <- c("State Total", "Gap", "Grand Total", "Unimmunized")
    
    data_raw <- readxl::read_excel(raw_path, sheet = sh)
    data_raw %>%
      transmute(
        county = County,
        school_year = Year,
        grade = grade_label,
        school_year = str_trim(as.character(school_year)),
        county = str_squish(as.character(county)),
        year_start = parse_start_year(school_year),
        time = as.Date(if_else(!is.na(year_start), paste0(year_start, "-09-01"), NA_character_)),
        N_total = readr::parse_number(as.character(get_col(data_raw, c("Number of Children")))),
        pct_dtap = parse_pct(get_col(data_raw, c("5\n DTaP", "5 DTaP"))),
        pct_polio = parse_pct(get_col(data_raw, c("4\n Polio", "4 Polio"))),
        pct_mmr = parse_pct(get_col(data_raw, c("2\n MMR", "2 MMR"))),
        pct_hep_b = parse_pct(get_col(data_raw, c("3\n Hep B", "3 Hep B"))),
        pct_varicella = parse_pct(get_col(data_raw, c("2\n Varicella", "2 Varicella"))),
        pct_tdap = parse_pct(get_col(data_raw, c("1 Tdap"))),
        pct_menacwy = parse_pct(get_col(data_raw, c("1 MenACWY"))),
        pct_medical_exempt = parse_pct(get_col(data_raw, c("Medical Exemption"))),
        pct_religious_exempt = parse_pct(get_col(data_raw, c("Religious Exemption"))),
        pct_full_exempt = parse_pct(get_col(data_raw, c("Total Exemption")))
      ) %>%
      filter(
        !is.na(time),
        !is.na(county),
        !(county %in% excluded_rows)
      )
  }
  
  data_all <- bind_rows(
    load_county_sheet(kg_sheet, "Kindergarten"),
    load_county_sheet(g7_sheet, "7th grade")
  )
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "MA") %>%
    mutate(geography_name = gsub(" County", "", geography_name))
  
  data_out <- data_all %>%
    left_join(
      fips_df %>% filter(nchar(geography) == 5),
      by = c("county" = "geography_name")
    ) %>%
    mutate(
      geography_name = county,
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_religious_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      N_tdap = NA_real_,
      N_menacwy = NA_real_,
      N_full_exempt = NA_real_
    ) %>%
    filter(!is.na(geography)) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_religious_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_religious_exempt, pct_medical_exempt, pct_full_exempt,
      N_total, N_tdap, pct_tdap, N_menacwy, pct_menacwy
    )
  
  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data_out, "./standard/data.csv")
  vroom::vroom_write(data_out, "./standard/data.csv.gz")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
