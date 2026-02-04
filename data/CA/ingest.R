library(dcf)
library(tidyverse)

select.state = 'CA'

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw", "csv", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()

# process raw if state has changed
if (!identical(process$raw_state, raw_state)) {
  
  
  ## CODE TO CLEAN RAW DATA 
  library(dcf)
  library(tidyverse)
  library(readxl)
  library(vroom)
  
  
  #### Helpers
  
  # Convert year to "year-month-day"
  schoolyear_to_time <- function(x) {
    x <- as.character(x)
    x <- str_replace_all(x, "\u2013|\u2014", "-") 
    yr <- str_extract(x, "^\\s*\\d{4}")
    as.Date(paste0(yr, "-09-01"))
  }
  
  clean_county <- function(x) {
    x <- as.character(x)
    x <- str_replace_all(x, "\\s+", " ")
    x <- str_trim(x)
    # normalize some common variants
    x <- str_replace_all(x, "\\s+County\\s*$", "")
    x <- if_else(x %in% c("State Total", "California", "CA Total", "Total"), "Total", x)
    x
  }
  
  # robust numeric parse (handles "*", "—", blanks, etc.)
  num <- function(x) readr::parse_number(as.character(x), na = c("", "NA", "N/A", "*", "—", "-", "–"))
  
  # Load US county FIPS from repo resources
  load_fips <- function() {
    # From state folder: data/CA -> ../../resources/all_fips.csv.gz
    fips_path <- file.path("..", "..", "resources", "all_fips.csv.gz")
    if (!file.exists(fips_path)) {
      stop("Could not find resources/all_fips.csv.gz at: ", fips_path,
           "\nMake sure you opened the CA project (data/CA) or setwd('data/CA').")
    }
    vroom::vroom(fips_path, show_col_types = FALSE)
  }
  
  # Find sheets by keyword (case-insensitive); error if none
  find_sheet <- function(sheets, pattern) {
    hit <- sheets[str_detect(tolower(sheets), tolower(pattern))]
    if (length(hit) == 0) {
      stop("Could not find sheet matching pattern: '", pattern, "'.\nAvailable sheets:\n- ",
           paste(sheets, collapse = "\n- "))
    }
    hit[1]
  }
  
  
  #### Process-guard
  
  # This version re-runs if raw files change OR ingest.R changes.
  raw_files <- list.files("raw", recursive = TRUE, full.names = TRUE)
  
  raw_state <- as.list(tools::md5sum(raw_files))
  raw_state$`ingest.R` <- as.character(tools::md5sum("ingest.R"))
  
  process <- dcf::dcf_process_record()
  
  if (!identical(process$raw_state, raw_state)) {
    
    
    #### Locate the CA workbook
    xlsx_path <- raw_files[str_detect(tolower(raw_files), "\\.xlsx$|\\.xls$")][1]
    if (is.na(xlsx_path) || !file.exists(xlsx_path)) {
      stop("No Excel file found under raw/. Put the CA workbook under data/CA/raw/")
    }
    
    sheets <- readxl::excel_sheets(xlsx_path)
    # KG sheets
    kg_sheet <- find_sheet(sheets, "kindergarten by county 19-23")
    
    # 7th grade sheets
    tdap_sheet <- find_sheet(sheets, "^7th grade 19-22 \\(Tdap\\)$")
    var_sheet  <- find_sheet(sheets, "^7th grade 19-22 \\(Varicella\\)$")
    
    
    #### Read and clean KG county sheet
    
    kg_raw <- readxl::read_excel(xlsx_path, sheet = kg_sheet, skip = 1)
    
    # normalize names
    kg_raw <- janitor::clean_names(kg_raw)
    
    needed_kg <- c(
      "county", "school_year", "grade",
      "total_number_students",
      "students_with_all_required_immunizations",
      "conditional_entrants",
      "students_with_a_permanent_medical_exemption",
      "others_lacking_required_immunizations",
      "overdue"
    )
    
    missing_kg <- setdiff(needed_kg, names(kg_raw))
    if (length(missing_kg) > 0) {
      stop(
        "KG sheet is missing expected columns: ", paste(missing_kg, collapse = ", "),
        "\nAvailable columns: ", paste(names(kg_raw), collapse = ", ")
      )
    }
    
    kg <- kg_raw %>%
      transmute(
        county = clean_county(county),
        school_year = as.character(school_year),
        grade = as.character(grade),
        N_students = num(total_number_students),
        pct_all_required = num(students_with_all_required_immunizations) * 100,
        pct_conditional = num(conditional_entrants) * 100,
        pct_pme = num(students_with_a_permanent_medical_exemption) * 100,
        pct_other_lacking = num(others_lacking_required_immunizations) * 100,
        pct_overdue = num(overdue) * 100,
        time = schoolyear_to_time(school_year)
      ) %>%
      filter(!is.na(county), !is.na(time))
    
    # Add geography (county FIPS)
    fips_df <- load_fips() %>%
      filter(state == "CA") %>%
      mutate(geography_name = clean_county(geography_name))
    
    kg_out <- kg %>%
      left_join(fips_df, by = c("county" = "geography_name")) %>%
      mutate(
        geography_name = if_else(is.na(geography), "Total", county),
        geography = if_else(is.na(geography), "06", geography)
      ) %>%
      transmute(
        geography,
        geography_name,
        time,
        grade,
        N_students,
        pct_all_required,
        pct_conditional,
        pct_pme,
        pct_other_lacking,
        pct_overdue
      ) %>%
      arrange(time, geography_name)
    
    
    #### Read and clean 7th-grade county sheets (Tdap / Varicella)
    
    # The 7th county sheets often have multi-row headers -> safest:
    # read with col_names = FALSE and then map columns by position.
    read_7th_by_county <- function(path, sheet, vax_name) {
      raw <- readxl::read_excel(path, sheet = sheet, skip = 4, col_names = FALSE)
      raw <- raw %>% slice(-1) # drop the leftover header row
      
      # make stable colnames
      names(raw) <- paste0("V", seq_len(ncol(raw)))
      
      # Expected structure (by position) from your earlier working version:
      # V1=state, V2=county, V3=school_year, V4=grade, V5=N, V6=pct_vax, V7=pct_conditional,
      # V8=pct_pme, V9=pct_other_lacking, V10=pct_overdue
      if (ncol(raw) < 10) {
        stop("7th-grade sheet '", sheet, "' has fewer columns than expected (need >= 10).")
      }
      
      out <- raw %>%
        transmute(
          county = clean_county(.data$V2),
          school_year = as.character(.data$V3),
          grade = as.character(.data$V4),
          N_students = num(.data$V5),
          pct_entrants_vax = num(.data$V6) * 100,
          pct_conditional = num(.data$V7) * 100,
          pct_pme = num(.data$V8) * 100,
          pct_other_lacking = num(.data$V9) * 100,
          pct_overdue = num(.data$V10) * 100,
          time = schoolyear_to_time(school_year),
          vax = vax_name
        ) %>%
        filter(!is.na(county), !is.na(time)) %>%
        filter(grade %in% c("7", "7th", "7th grade", "7th Grade"))
      
      out
    }
    
    g7_tdap <- read_7th_by_county(xlsx_path, tdap_sheet, "tdap")
    g7_var  <- read_7th_by_county(xlsx_path, var_sheet,  "varicella")
    
    g7_long <- bind_rows(g7_tdap, g7_var) %>%
      left_join(fips_df, by = c("county" = "geography_name")) %>%
      mutate(
        geography_name = if_else(is.na(geography), "Total", county),
        geography = if_else(is.na(geography), "06", geography)
      ) %>%
      select(geography, geography_name, time, grade, vax,
             N_students, pct_entrants_vax, pct_conditional, pct_pme, pct_other_lacking, pct_overdue)
    
    # Wide: vaccine-specific columns like N_students_tdap, pct_entrants_vax_tdap, etc.
    g7_out <- g7_long %>%
      tidyr::pivot_wider(
        id_cols = c(geography, geography_name, time, grade),
        names_from = vax,
        values_from = c(
          N_students,
          pct_entrants_vax,
          pct_conditional,
          pct_pme,
          pct_other_lacking,
          pct_overdue
        ),
        names_glue = "{.value}_{vax}"
      ) %>%
      arrange(time, geography_name)
    

    #### Write outputs

    dir.create("standard", showWarnings = FALSE)
    
    # 1) KG output
    vroom::vroom_write(kg_out, "./standard/data_kg.csv.gz")
    
    # 2) 7th grade output
    vroom::vroom_write(g7_out, "./standard/data_7th.csv.gz")
    
    # 3) Combined output (for dcf conventions)
    # (Different columns across KG vs 7th are OK; they’ll be NA where not applicable.)
    data <- bind_rows(
      kg_out %>% mutate(source_grade = "KG"),
      g7_out %>% mutate(source_grade = "7th")
    )
    
    vroom::vroom_write(data, "./standard/data.csv.gz")
    }
    
    # record processed raw state
    process$raw_state <- raw_state
    dcf::dcf_process_record(updated = process)}