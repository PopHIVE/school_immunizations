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
  
  raw_path <- "./raw/KY_2020-2025.xlsx"
  sheets <- c("Kindergarten", "Seventh Grade", "Eleventh Grade")

  # Each sheet publishes two side-by-side tables: a "Medical Exemption &
  # Religious Exemption Rates" block (ME%/RO% per year) and an "Overall
  # Vaccine Exemption Rates" block (one total% per year). The first block has
  # an extra sub-header row (the ME%/RO% labels) that the second doesn't, so
  # its data starts one row lower in the sheet -- reading both with the same
  # `skip` shifts the medical/religious block's county names up by one county
  # relative to its own values. The two blocks are therefore parsed
  # separately, each against its own county column, and joined back together
  # on county + school year rather than on raw row position.
  data_all <- bind_rows(lapply(sheets, function(sh) {
    header_rows <- readxl::read_excel(raw_path, sheet = sh, n_max = 4, col_names = FALSE)
    header_row2 <- as.character(header_rows[2, ])
    header_row3 <- as.character(header_rows[3, ])
    header_row4 <- as.character(header_rows[4, ])

    county_cols <- which(header_row2 == "County")
    if (length(county_cols) != 2) {
      stop("KY '", sh, "': expected 2 'County' columns, found ", length(county_cols))
    }
    breakdown_col <- county_cols[1]
    overall_col <- county_cols[2]

    # Block 1 (ME%/RO%): the year label sits only on the ME% column of each
    # pair in the source's merged header cell; the RO% column reads NA.
    # Carry it forward so both columns of the pair get a year.
    block1_cols <- seq(breakdown_col + 1, overall_col - 1)
    year_filled <- header_row3
    for (i in block1_cols) {
      if (i > 1 && is.na(year_filled[i]) && !is.na(header_row4[i])) {
        year_filled[i] <- year_filled[i - 1]
      }
    }
    breakdown_year_cols <- block1_cols[!is.na(header_row4[block1_cols]) &
                                          str_detect(header_row4[block1_cols], "^(ME|RO)%$")]

    breakdown_raw <- readxl::read_excel(raw_path, sheet = sh, skip = 4, col_names = FALSE)
    breakdown_sel <- breakdown_raw[, c(breakdown_col, breakdown_year_cols), drop = FALSE]
    names(breakdown_sel) <- c(
      "county",
      paste(year_filled[breakdown_year_cols], header_row4[breakdown_year_cols], sep = "|")
    )
    breakdown_long <- breakdown_sel %>%
      mutate(county = str_trim(as.character(county))) %>%
      filter(!is.na(county) & county != "") %>%
      mutate(across(-county, as.character)) %>%
      pivot_longer(-county, names_to = "year_type", values_to = "value") %>%
      separate(year_type, into = c("school_year", "type"), sep = "\\|") %>%
      mutate(value = readr::parse_number(value)) %>%
      pivot_wider(names_from = type, values_from = value) %>%
      rename(pct_medical_exempt = `ME%`, pct_religious_exempt = `RO%`)

    # Block 2 (Overall): one total% column per year, no sub-header row.
    overall_year_cols <- which(str_detect(header_row3, "^\\d{4}-\\d{4}$") &
                                  seq_along(header_row3) > overall_col)
    overall_raw <- readxl::read_excel(raw_path, sheet = sh, skip = 3, col_names = FALSE)
    overall_sel <- overall_raw[, c(overall_col, overall_year_cols), drop = FALSE]
    names(overall_sel) <- c("county", header_row3[overall_year_cols])
    overall_long <- overall_sel %>%
      mutate(county = str_trim(as.character(county))) %>%
      filter(!is.na(county) & county != "") %>%
      mutate(across(-county, as.character)) %>%
      pivot_longer(-county, names_to = "school_year", values_to = "pct_full_exempt") %>%
      mutate(pct_full_exempt = readr::parse_number(pct_full_exempt))

    full_join(breakdown_long, overall_long, by = c("county", "school_year")) %>%
      mutate(
        grade = sh,
        year_end = str_extract(school_year, "\\d{4}$"),
        time = as.Date(school_year_time_from_end(year_end))
      ) %>%
      filter(!is.na(time))
  }))

  # The sheets run on past the last county with blank rows; those carried no
  # county at all and used to be emitted as 18 phantom statewide rows.
  data_out <- data_all %>%
    join_county_fips("KY", drop_na = TRUE) %>%
    mutate(
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_religious_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = NA_real_,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_
    ) %>%
    transmute(
      time, geography, geography_name, grade,
      N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
      N_religious_exempt, N_medical_exempt, N_full_exempt,
      pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
      pct_religious_exempt, pct_medical_exempt, pct_full_exempt
    )
  
  write_standard(data_out, "Kentucky", "./standard/data.csv.gz", from = "percent")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
