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
  
  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)
  
  parse_file <- function(path) {
    year_match <- str_match(basename(path), "(\\d{4})-(\\d{2})")
    year_end <- if (!is.na(year_match[2]) && !is.na(year_match[3])) {
      paste0(substr(year_match[2], 1, 2), year_match[3])
    } else {
      NA_character_
    }
    time <- if (!is.na(year_end)) as.Date(paste0(year_end, "-09-01")) else as.Date(NA)
    if (is.na(time)) return(tibble())
    
    sheets <- readxl::excel_sheets(path)
    county_sheets <- sheets[sheets != "Document map"]
    
    bind_rows(lapply(county_sheets, function(sh) {
      df <- readxl::read_excel(path, sheet = sh, col_names = FALSE)
      if (nrow(df) == 0) return(tibble())
      
      header_idx <- which(as.character(df[[1]]) == "Grade")[1]
      if (is.na(header_idx)) return(tibble())
      header <- as.character(df[header_idx, ])
      
      col_dtap <- which(str_detect(header, "DTaP"))
      col_polio <- which(str_detect(header, "Polio"))
      col_mmr <- which(str_detect(header, "MMR"))
      col_hep_b <- which(str_detect(header, "HEP"))
      col_varicella2 <- which(str_detect(header, "Varicella 2 Doses"))
      col_varicella <- if (length(col_varicella2) > 0) col_varicella2 else which(str_detect(header, "Varicella"))
      col_med <- which(str_detect(header, "Medical") & str_detect(header, "Exempt"))
      col_rel <- which(str_detect(header, "Religious") & str_detect(header, "Exempt"))
      col_phil <- which(str_detect(header, "Philoso"))
      
      data_rows <- df[(header_idx + 1):nrow(df), , drop = FALSE]
      grade_rows <- which(str_detect(as.character(data_rows[[1]]), "Kindergarten|7th|12th"))
      
      bind_rows(lapply(grade_rows, function(i) {
        grade_label <- as.character(data_rows[[1]][i])
        percent_row <- data_rows[i + 1, , drop = FALSE]
        if (!str_detect(as.character(percent_row[[1]]), "Percent")) return(tibble())
        
        get_val <- function(idx) {
          if (length(idx) == 0) return(NA_real_)
          readr::parse_number(as.character(percent_row[[idx[1]]]))
        }
        
        pct_med <- get_val(col_med)
        pct_rel <- get_val(col_rel)
        pct_phil <- get_val(col_phil)
        
        tibble(
          county = sh,
          grade = case_when(
            str_detect(grade_label, "Kindergarten") ~ "Kindergarten",
            str_detect(grade_label, "7th") ~ "7th grade",
            str_detect(grade_label, "12th") ~ "12th grade",
            TRUE ~ grade_label
          ),
          pct_dtap = get_val(col_dtap),
          pct_polio = get_val(col_polio),
          pct_mmr = get_val(col_mmr),
          pct_hep_b = get_val(col_hep_b),
          pct_varicella = get_val(col_varicella),
          pct_medical_exempt = pct_med,
          pct_personal_exempt = sum(c(pct_rel, pct_phil), na.rm = TRUE),
          time = time
        )
      }))
    }))
  }
  
  data_all <- bind_rows(lapply(raw_files, parse_file))
  
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "PA") %>%
    mutate(geography_name = gsub(" County", "", geography_name))
  
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
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
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
