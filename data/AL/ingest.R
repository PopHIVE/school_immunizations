#
# Download
#

# add files to the `raw` directory

#
# Reformat
#

# read from the `raw` directory, and write to the `standard` directory

# --- activate renv no matter where this script is run from ---

# Find project root by walking up until we see renv.lock

library(dcf)
library(tidyverse)
library(readxl)
library(dplyr)
library(stringr)
library(vroom)
library(readr)

## change here the 2 digit code being processed here
select.state = 'AL'

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw", "xlsx", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()

# process raw if state has changed
if (!identical(process$raw_state, raw_state)) {
  
  # ---- helpers ----
  parse_pct_points <- function(x) {
    # returns percent points (0-100)
    if (is.numeric(x)) {
      # Excel percent often imported as proportion (0-1); if >1 assume already percent points
      return(ifelse(is.na(x), NA_real_, ifelse(x > 1, x, x * 100)))
    }
    y <- readr::parse_number(as.character(x))  # "1.88%" -> 1.88
    ifelse(is.na(y), NA_real_, y)              # already percent points
  }
  
  parse_num <- function(x) {
    if (is.numeric(x)) return(as.numeric(x))
    readr::parse_number(as.character(x))
  }
  
  infer_grade <- function(path) {
    fn <- tolower(basename(path))
    if (str_detect(fn, "kindergarten")) return("Kindergarten")
    if (str_detect(fn, "seventh"))      return("7th grade")
    if (str_detect(fn, "ninth"))        return("9th grade")
    NA_character_
  }
  
  raw_dir <- "./raw"
  xlsx_files <- list.files(raw_dir, pattern = "\\.xlsx$", full.names = TRUE)
  
  # ---- FIPS ----
  fips_df <- vroom::vroom("../../resources/all_fips.csv.gz") %>%
    filter(state == "AL") %>%
    mutate(
      geography_name = gsub(" County", "", geography_name),
      geography_name = str_to_title(str_to_lower(geography_name)),
      geography = as.character(geography) # keep as character
    )
  
  process_one_workbook <- function(xlsx_path) {
    grade_label <- infer_grade(xlsx_path)
    if (is.na(grade_label)) stop("Could not infer grade from filename: ", basename(xlsx_path))
    
    sheets <- readxl::excel_sheets(xlsx_path)
    
    bind_rows(lapply(sheets, function(sh) {
      df <- readxl::read_excel(xlsx_path, sheet = sh)
      
      # Normalize headers (remove embedded line breaks / tabs)
      names(df) <- names(df) %>%
        str_replace_all("\\s+", " ") %>%
        str_trim()
      
      sheet_date <- as.Date(sh, format = "%m.%d.%Y")
      if (is.na(sheet_date)) stop("Sheet name is not mm.dd.yyyy: '", sh, "' in ", basename(xlsx_path))
      yearpart <- format(sheet_date, "%Y")
      
      df %>%
        transmute(
          time = sheet_date,
          geography_name = str_to_title(str_to_lower(County)),
          grade = grade_label,
          
          # denominator (kept for reference / debugging; not used for N_dtap etc.)
          N_students = parse_num(`# of Students`),
          
          # ORIGINAL exemption component COUNTS (keep them)
          n_full_med  = parse_num(`# with Full Medical Exemption`),
          n_part_med  = parse_num(`# UTD with Partial Medical Exemption`),
          n_full_rel  = parse_num(`# with Full Religous Exemption`),
          n_part_rel  = parse_num(`# UTD with Partial Religious Exemption`),
          
          # ORIGINAL exemption component PCTS (keep them, as percent points)
          pct_full_med = parse_pct_points(`% with Full Medical Exemption`),
          pct_part_med = parse_pct_points(`% UTD with Partial Medical Exemtion`),
          pct_full_rel = parse_pct_points(`% with Full Religous Exemption`),
          pct_part_rel = parse_pct_points(`% UTD with Partial Religious Exemption`)
        )
    }))
  }
  
  data_all <- lapply(xlsx_files, process_one_workbook) %>%
    bind_rows() %>%
    left_join(fips_df, by = "geography_name") %>%
    mutate(
      # --- REQUIRED vaccine columns should be NA ---
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      
      # --- Keep original component exemption info as separate columns ---
      N_full_medical_exempt = n_full_med,
      pct_full_medical_exempt = pct_full_med,
      
      N_partial_medical_exempt_utd = n_part_med,
      pct_partial_medical_exempt_utd = pct_part_med,
      
      N_full_religious_exempt = n_full_rel,
      pct_full_religious_exempt = pct_full_rel,
      
      N_partial_religious_exempt_utd = n_part_rel,
      pct_partial_religious_exempt_utd = pct_part_rel,
      
      # --- Map to your standard "exempt" columns (NOT from # students) ---
      # You didn't ask for totals here, so we map them to the original components.
      # If you prefer totals, tell me and I’ll switch.
      N_medical_exempt = n_full_med,
      pct_medical_exempt = pct_full_med,
      
      N_personal_exempt = n_full_rel,
      pct_personal_exempt = pct_full_rel,
      
      # Per your instruction: full_exempt should equal FULL RELIGIOUS (count and pct)
      N_full_exempt = n_full_rel,
      pct_full_exempt = pct_full_rel
    ) %>%
    # optional: round key pct columns to 2 decimals
    mutate(
      across(
        c(pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
          pct_full_medical_exempt, pct_partial_medical_exempt_utd,
          pct_full_religious_exempt, pct_partial_religious_exempt_utd),
        ~ round(.x, 2)
      )
    ) %>%
    # Final column order: match your standard header first, then keep component details
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_personal_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_personal_exempt, pct_medical_exempt, pct_full_exempt,
      
      # extra preserved component columns
      N_full_medical_exempt, pct_full_medical_exempt,
      N_partial_medical_exempt_utd, pct_partial_medical_exempt_utd,
      N_full_religious_exempt, pct_full_religious_exempt,
      N_partial_religious_exempt_utd, pct_partial_religious_exempt_utd
    )
  
  # Debug: unmatched counties (if any)
  print(data_all %>% filter(is.na(geography)) %>% distinct(geography_name) %>% head(50))
  
  vroom::vroom_write(data_all, "./standard/data.csv.gz")
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}






