library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
library(purrr)
source("../../resources/add_state_column.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  parse_time <- function(path) {
    year_match <- str_match(basename(path), "(20\\d{2})-(\\d{2})")
    if (is.na(year_match[1, 2])) {
      return(as.Date(NA))
    }

    as.Date(paste0(year_match[1, 2], "-09-01"))
  }

  normalize_sheet <- function(path, sheet) {
    read_excel(path, sheet = sheet, col_names = FALSE) %>%
      mutate(across(everything(), ~ str_squish(as.character(.x))))
  }

  extract_county_name <- function(x) {
    county <- str_match(x, "County:\\s*\\d+\\s*-\\s*(.+)$")[, 2]
    county %>%
      str_replace("\\s+(DTaP/|Total|Students|Grade)\\b.*$", "") %>%
      str_squish()
  }

  extract_numeric_sequence <- function(row_values) {
    vals <- row_values[!is.na(row_values) & row_values != ""]
    if (length(vals) == 0) {
      return(numeric())
    }

    nums <- unlist(str_extract_all(
      vals,
      "-?\\d[\\d,]*(?:\\.\\d+)?(?:E-?\\d+)?"
    ))

    if (length(nums) == 0) {
      return(numeric())
    }

    readr::parse_number(nums, locale = locale(grouping_mark = ","))
  }

  build_record <- function(county, grade, counts, pcts, time) {
    base_counts <- list(
      total_enrolled = counts[1],
      N_dtap = counts[2],
      N_polio = counts[3],
      N_mmr = counts[4],
      N_hep_b = counts[5],
      N_varicella = counts[7]
    )

    base_pcts <- list(
      pct_dtap = pcts[1] * 100,
      pct_polio = pcts[2] * 100,
      pct_mmr = pcts[3] * 100,
      pct_hep_b = pcts[4] * 100,
      pct_varicella = pcts[6] * 100
    )

    if (grade == "Kindergarten") {
      med_idx <- 8
      rel_idx <- 9
      phil_idx <- 10
    } else if (grade == "7th Grade") {
      med_idx <- 10
      rel_idx <- 11
      phil_idx <- 12
    } else if (grade == "12th Grade") {
      med_idx <- 11
      rel_idx <- 12
      phil_idx <- 13
    } else {
      med_idx <- 12
      rel_idx <- 13
      phil_idx <- 14
    }

    n_medical <- counts[med_idx]
    n_religious <- counts[rel_idx]
    n_philosophical <- counts[phil_idx]
    pct_medical <- pcts[med_idx - 1] * 100
    pct_religious <- pcts[rel_idx - 1] * 100
    pct_philosophical <- pcts[phil_idx - 1] * 100

    tibble(
      county = county,
      time = time,
      grade = grade,
      total_enrolled = base_counts$total_enrolled,
      N_dtap = base_counts$N_dtap,
      N_polio = base_counts$N_polio,
      N_mmr = base_counts$N_mmr,
      N_hep_b = base_counts$N_hep_b,
      N_varicella = base_counts$N_varicella,
      N_medical_exempt = n_medical,
      N_personal_exempt = coalesce(n_religious, 0) + coalesce(n_philosophical, 0),
      N_full_exempt = coalesce(n_medical, 0) + coalesce(n_religious, 0) + coalesce(n_philosophical, 0),
      pct_dtap = base_pcts$pct_dtap,
      pct_polio = base_pcts$pct_polio,
      pct_mmr = base_pcts$pct_mmr,
      pct_hep_b = base_pcts$pct_hep_b,
      pct_varicella = base_pcts$pct_varicella,
      pct_medical_exempt = pct_medical,
      pct_personal_exempt = coalesce(pct_religious, 0) + coalesce(pct_philosophical, 0),
      pct_full_exempt = coalesce(pct_medical, 0) + coalesce(pct_religious, 0) + coalesce(pct_philosophical, 0)
    )
  }

  compute_totals <- function(df) {
    total_enrolled <- sum(df$total_enrolled, na.rm = TRUE)

    tibble(
      county = first(df$county),
      time = first(df$time),
      grade = "Totals",
      total_enrolled = total_enrolled,
      N_dtap = sum(df$N_dtap, na.rm = TRUE),
      N_polio = sum(df$N_polio, na.rm = TRUE),
      N_mmr = sum(df$N_mmr, na.rm = TRUE),
      N_hep_b = sum(df$N_hep_b, na.rm = TRUE),
      N_varicella = sum(df$N_varicella, na.rm = TRUE),
      N_medical_exempt = sum(df$N_medical_exempt, na.rm = TRUE),
      N_personal_exempt = sum(df$N_personal_exempt, na.rm = TRUE),
      N_full_exempt = sum(df$N_full_exempt, na.rm = TRUE)
    ) %>%
      mutate(
        pct_dtap = if_else(total_enrolled > 0, 100 * N_dtap / total_enrolled, NA_real_),
        pct_polio = if_else(total_enrolled > 0, 100 * N_polio / total_enrolled, NA_real_),
        pct_mmr = if_else(total_enrolled > 0, 100 * N_mmr / total_enrolled, NA_real_),
        pct_hep_b = if_else(total_enrolled > 0, 100 * N_hep_b / total_enrolled, NA_real_),
        pct_varicella = if_else(total_enrolled > 0, 100 * N_varicella / total_enrolled, NA_real_),
        pct_medical_exempt = if_else(total_enrolled > 0, 100 * N_medical_exempt / total_enrolled, NA_real_),
        pct_personal_exempt = if_else(total_enrolled > 0, 100 * N_personal_exempt / total_enrolled, NA_real_),
        pct_full_exempt = if_else(total_enrolled > 0, 100 * N_full_exempt / total_enrolled, NA_real_)
      )
  }

  parse_county_block <- function(df, county_row, next_county_row, time) {
    county <- extract_county_name(df[[1]][county_row])
    if (is.na(county) || county == "") {
      return(tibble())
    }

    block_end <- min(next_county_row - 1, nrow(df))
    block <- df[county_row:block_end, , drop = FALSE]

    grade_idx <- which(str_detect(block[[1]], "^(Kindergarten|7th Grade|12th Grade|Totals:)"))
    if (length(grade_idx) == 0) {
      return(tibble())
    }

    records <- map_dfr(grade_idx, function(idx) {
      grade_cell <- block[[1]][idx]
      grade <- case_when(
        str_detect(grade_cell, "^Kindergarten") ~ "Kindergarten",
        str_detect(grade_cell, "^7th Grade") ~ "7th Grade",
        str_detect(grade_cell, "^12th Grade") ~ "12th Grade",
        str_detect(grade_cell, "^Totals:") ~ "Totals",
        TRUE ~ NA_character_
      )

      if (is.na(grade)) {
        return(tibble())
      }

      pct_idx <- idx + 1
      if (pct_idx > nrow(block) || !str_detect(block[[1]][pct_idx], "^Percent")) {
        return(tibble())
      }

      counts <- extract_numeric_sequence(block[idx, , drop = TRUE])
      pcts <- extract_numeric_sequence(block[pct_idx, , drop = TRUE])

      if (grade == "7th Grade" && length(counts) > 0 && counts[1] == 7) {
        counts <- counts[-1]
      }

      if (grade == "12th Grade" && length(counts) > 0 && counts[1] == 12) {
        counts <- counts[-1]
      }

      if (length(counts) < 7 || length(pcts) < 6) {
        return(tibble())
      }

      build_record(county, grade, counts, pcts, time)
    })

    if (nrow(records) == 0) {
      return(tibble())
    }

    if (!"Totals" %in% records$grade) {
      records <- bind_rows(records, compute_totals(filter(records, grade != "Totals")))
    }

    records %>%
      filter(grade %in% c("Kindergarten", "7th Grade", "12th Grade", "Totals"))
  }

  parse_table_sheet <- function(path, sheet = "Table 1") {
    df <- normalize_sheet(path, sheet)
    time <- parse_time(path)

    county_rows <- which(str_detect(df[[1]], "^County:"))
    if (length(county_rows) == 0) {
      return(tibble())
    }

    next_rows <- c(county_rows[-1], nrow(df) + 1)
    map2_dfr(county_rows, next_rows, ~ parse_county_block(df, .x, .y, time))
  }

  parse_county_sheets <- function(path) {
    time <- parse_time(path)
    sheets <- excel_sheets(path)
    county_sheets <- sheets[sheets != "Document map"]

    map_dfr(county_sheets, function(sheet) {
      df <- normalize_sheet(path, sheet)
      county_row <- which(str_detect(df[[1]], "^County:"))[1]

      if (is.na(county_row)) {
        return(tibble())
      }

      parse_county_block(df, county_row, nrow(df) + 1, time)
    })
  }

  parse_file <- function(path) {
    sheets <- excel_sheets(path)
    if ("Table 1" %in% sheets && length(sheets) == 1) {
      return(parse_table_sheet(path, "Table 1"))
    }

    if ("Table 1" %in% sheets && length(sheets) > 1) {
      return(parse_table_sheet(path, "Table 1"))
    }

    parse_county_sheets(path)
  }

  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)
  data_all <- bind_rows(lapply(raw_files, parse_file))

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_df <- all_fips %>%
    filter(state == "PA", nchar(geography) == 5) %>%
    mutate(geography_name = str_remove(geography_name, " County$"))

  pa_counties <- fips_df$geography_name

  normalize_county <- function(x) {
    if (is.na(x) || x == "") {
      return(x)
    }

    exact <- pa_counties[pa_counties == x]
    if (length(exact) == 1) {
      return(exact)
    }

    matches <- pa_counties[str_detect(x, paste0("^", pa_counties, "\\b"))]
    if (length(matches) > 0) {
      return(matches[[which.max(nchar(matches))]])
    }

    x
  }

  data_all <- data_all %>%
    mutate(county = vapply(county, normalize_county, character(1)))

  grade_levels <- c("Kindergarten", "7th Grade", "12th Grade", "Totals")

  data_out <- data_all %>%
    left_join(fips_df, by = c("county" = "geography_name")) %>%
    mutate(
      geography_name = county,
      grade = factor(grade, levels = grade_levels)
    ) %>%
    arrange(time, geography_name, grade) %>%
    transmute(
      time,
      geography,
      geography_name,
      grade = as.character(grade),
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
      pct_full_exempt,
      total_enrolled
    )

  vroom::vroom_write(add_state_column(data_out, "Pennsylvania"), "./standard/data.csv.gz")
  vroom::vroom_write(add_state_column(data_out, "Pennsylvania"), "./standard/data.csv")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
