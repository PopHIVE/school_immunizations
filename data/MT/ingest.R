source("../../resources/rate_scale.R")
source("../../resources/county_fips.R")
# =============================================================================
# MT - County Immunization Coverage (2020) and Exemption Rates (2016-2019)
# =============================================================================

library(dcf)
library(dplyr)
library(tibble)
library(stringr)
library(readxl)
library(readr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_pct_points <- function(x) {
  y <- readr::parse_number(as.character(x))
  if (all(is.na(y))) return(y)
  if (max(y, na.rm = TRUE) <= 1) return(y * 100)
  y
}

parse_table <- function(path) {
  d <- readxl::read_excel(path, sheet = 1, col_names = FALSE)
  idx <- which(d[[1]] == "County")
  if (length(idx) == 0) return(NULL)

  header <- as.character(d[idx[1], ])
  data_raw <- d[(idx[1] + 1):nrow(d), ]
  names(data_raw) <- header

  data_raw %>%
    filter(!is.na(County)) %>%
    transmute(
      county = str_to_title(str_trim(County)),
      n_assessed = readr::parse_number(as.character(`Number\r\nAssessed`)),
      pct_utd = parse_pct_points(`%\r\nUTD`),
      pct_dtap = parse_pct_points(`% W/\r\nDTaP4`),
      pct_polio = parse_pct_points(`% W/\r\nPolio 3`),
      pct_mmr = parse_pct_points(`% W/\r\nMMR 1`),
      pct_hib_utd = parse_pct_points(`% W/\r\nHib UTD`),
      pct_hep_b = parse_pct_points(`% W/\r\nHep B 3`),
      pct_varicella = parse_pct_points(`% W/\r\nVar 1`),
      pct_pcv_utd = parse_pct_points(`% W/\r\nPCV UTD`)
    )
}

# County medical/religious exemption rates. The 2017-2018 and 2018-2019
# assessment reports each carry a "Table 3" with two academic years of county
# exemption rates side by side, so together they cover 2016-2017 through
# 2018-2019. The 2015-2016 and 2016-2017 reports only chart county exemption
# rates as images (Figures 3-4) with no backing data table, so those two
# years aren't recoverable from the raw files.
parse_exempt_table <- function(path) {
  d <- suppressMessages(read_excel(path, sheet = 1, col_names = FALSE))
  m <- as.matrix(d)

  header_rows <- which(apply(m, 1, function(r) {
    any(str_trim(r) == "COUNTY", na.rm = TRUE)
  }))
  if (length(header_rows) == 0) return(NULL)

  header <- str_trim(m[header_rows[1], ])
  county_col <- which(header == "COUNTY")[1]
  med_col <- which(grepl("^Medical Exemptions", header))[1]
  rel_col <- which(grepl("^Religious Exemptions", header))[1]

  sub_row <- header_rows[1] + 1
  med_year_cols <- (med_col:(rel_col - 1))[!is.na(m[sub_row, med_col:(rel_col - 1)])]
  rel_year_cols <- (rel_col:ncol(m))[!is.na(m[sub_row, rel_col:ncol(m)])]
  years <- str_extract(
    m[sub_row, c(med_year_cols[1:2], rel_year_cols[1:2])], "\\d{4}-\\d{4}"
  )

  # Data rows are ALL CAPS county names (plus the "MONTANA" statewide total);
  # that pattern is what distinguishes them from the narrative text, repeated
  # page-break headers, and legend rows sharing the same column.
  county_vals <- str_trim(m[, county_col])
  is_county_row <- grepl("^[A-Z][A-Z '.&-]*$", county_vals) &
    !is.na(county_vals) & county_vals != "COUNTY"
  montana_row <- which(county_vals == "MONTANA")[1]
  in_range <- seq_len(nrow(m)) >= header_rows[1] & seq_len(nrow(m)) <= montana_row
  rows <- which(is_county_row & in_range)

  bind_rows(
    tibble(
      county = county_vals[rows],
      time = as.Date(paste0(substr(years[1], 1, 4), "-09-01")),
      pct_medical_exempt = parse_number(m[rows, med_year_cols[1]]),
      pct_religious_exempt = parse_number(m[rows, rel_year_cols[1]])
    ),
    tibble(
      county = county_vals[rows],
      time = as.Date(paste0(substr(years[2], 1, 4), "-09-01")),
      pct_medical_exempt = parse_number(m[rows, med_year_cols[2]]),
      pct_religious_exempt = parse_number(m[rows, rel_year_cols[2]])
    )
  )
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_path <- "./raw/2020 Immunization Coverage  (1).xlsx"
  data_tbl <- parse_table(raw_path)

  if (!is.null(data_tbl)) {
    # The workbook continues past the county table with NIS comparison rows and
    # a block of footnotes, all of which land in the County column. They used to
    # be kept and filed under the state FIPS alongside McCone and Lewis & Clark,
    # which str_to_title() had mangled out of matching.
    data_out <- data_tbl %>%
      join_county_fips(
        "MT",
        drop = c("^notes?\\b", "^total records reviewed", "^additional records",
                 "^records not associated", "\\bnis\\b")
      ) %>%
      mutate(
        grade = "Overall",
        time = as.Date("2020-09-01"),
        N_dtap = NA_real_,
        N_polio = NA_real_,
        N_mmr = NA_real_,
        N_hep_b = NA_real_,
        N_varicella = NA_real_,
        N_religious_exempt = NA_real_,
        N_medical_exempt = NA_real_,
        N_full_exempt = NA_real_,
        pct_religious_exempt = NA_real_,
        pct_medical_exempt = NA_real_,
        pct_full_exempt = NA_real_
      ) %>%
      transmute(
        time, geography, geography_name, grade,
        N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
        N_religious_exempt, N_medical_exempt, N_full_exempt,
        pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
        pct_religious_exempt, pct_medical_exempt, pct_full_exempt,
        n_assessed, pct_utd, pct_hib_utd, pct_pcv_utd
      )

    # The 2018-2019 file's Table 3 is the more recently published revision of
    # 2017-2018, so it wins over the 2017-2018 file's own figures for that
    # overlapping year.
    exempt_out <- bind_rows(
      parse_exempt_table("./raw/2018-2019 M_R Exemptions (1).xlsx"),
      parse_exempt_table("./raw/2017-2018 M_R Exemptions.xlsx")
    ) %>%
      distinct(county, time, .keep_all = TRUE) %>%
      join_county_fips("MT", statewide = "Montana") %>%
      mutate(
        grade = "Overall",
        N_dtap = NA_real_, N_polio = NA_real_, N_mmr = NA_real_,
        N_hep_b = NA_real_, N_varicella = NA_real_,
        N_religious_exempt = NA_real_, N_medical_exempt = NA_real_,
        N_full_exempt = NA_real_,
        pct_dtap = NA_real_, pct_polio = NA_real_, pct_mmr = NA_real_,
        pct_hep_b = NA_real_, pct_varicella = NA_real_,
        pct_full_exempt = NA_real_,
        n_assessed = NA_real_, pct_utd = NA_real_,
        pct_hib_utd = NA_real_, pct_pcv_utd = NA_real_
      ) %>%
      transmute(
        time, geography, geography_name, grade,
        N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
        N_religious_exempt, N_medical_exempt, N_full_exempt,
        pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
        pct_religious_exempt, pct_medical_exempt, pct_full_exempt,
        n_assessed, pct_utd, pct_hib_utd, pct_pcv_utd
      )

    data_out <- bind_rows(data_out, exempt_out)

    dir.create("standard", showWarnings = FALSE)
    write_standard(data_out, "Montana", "standard/data.csv.gz", from = "percent")
  }

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
