source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
# =============================================================================
# VT - K-12 State and County Immunization & Exemption Percentages (2017-2025)
# =============================================================================

library(dcf)
library(dplyr)
library(stringr)
library(readxl)
library(readr)
library(tidyr)
library(vroom)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

# school_year_end_from_label() in resources/school_year.R. The local copy this
# replaces was not vectorised -- it returned the first row's year for the whole
# `School Year` column, so all eight school years in the workbook came out dated
# to 2017-18 and Vermont looked like a single-year source.

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_path <- "./raw/VT K-12 state and county-level data 2017 thru 2025.xlsx"
  raw <- readxl::read_excel(raw_path, sheet = "Sheet1", col_names = FALSE)

  header_idx <- which(raw[[1]] == "School Year")
  if (length(header_idx) > 0) {
    header <- as.character(raw[header_idx[1], ])
    data_raw <- raw[(header_idx[1] + 1):nrow(raw), ]
    names(data_raw) <- header

    data_long <- data_raw %>%
      filter(!is.na(`School Year`)) %>%
      transmute(
        school_year = as.character(`School Year`),
        unit = str_trim(`Unit of analysis`),
        public_independent = str_trim(`Public or Independent`),
        grade = str_trim(`Grades`),
        geography = str_trim(`Geography`),
        enrollment = readr::parse_number(as.character(`Enrollment`)),
        type = str_trim(`Immunization or Exemption`),
        exemption = str_trim(`Exemption`),
        immunization = str_trim(`Immunization`),
        # The workbook keeps two percent columns -- `Percent` (1299 rows) and
        # `Percentage` (1959) -- and both publish proportions, so the coalesced
        # column is declared "rate" rather than guessed at.
        #
        # 660 cells are top-coded ">95%" and another 353 carry "*", "**", "***"
        # or "N/A". readr::parse_number(">95%") returns 95, which used to push
        # the column max above 1 and thereby disable the old column-global
        # `max <= 1` rescale for every other row -- that is why pct_dtap shipped
        # values spanning 0.85 to 95. parse_rate() returns NA for a censored
        # cell; where the marker states a bound it is put back on the rate scale
        # below, so 95 percent lands as 0.95 and not as 95.
        raw_pct = coalesce(`Percent`, `Percentage`),
        censor = censor_direction(raw_pct),
        # Per-measure account of why a cell has no number: blank where the
        # workbook printed a figure, else "suppressed" (an asterisk run),
        # "missing" ("N/A") or "top_coded" (">95%"). Every cell in this workbook
        # carries a number or one of those three markers, so the flag is never
        # NA and a blank always means an actual published figure.
        flag = censor_flag(raw_pct),
        value = parse_rate(raw_pct, from = "rate"),
        # ">95%" is right-censored: the true value lies between 0.95 and 1.00.
        # The number kept is the one the workbook printed -- 95 percent, i.e.
        # 0.95 on this file's rate scale -- and nothing is substituted for it.
        # No estimate is invented, so the value never disagrees with the source;
        # flag_<measure> marks the cell "top_coded" so a consumer can exclude it
        # or model it as interval-censored on [0.95, 1.00] rather than reading
        # 0.95 as a measurement.
        #
        # The bound is parsed as PERCENT POINTS even though the numeric cells of
        # the same column are proportions: the workbook writes the marker as
        # ">95%" but a measured cell as 0.87. The check after this block fails
        # loudly if a marker ever arrives on the other scale.
        #
        # Undirected markers ("*", "N/A") state no bound and stay missing.
        bound = censor_bound(raw_pct, from = "percent"),
        value = if_else(!is.na(censor) & censor %in% c("left", "right"),
                        bound, value)
      )

    # A bound outside [0, 1] means the marker was not on the percent scale this
    # assumes, so the parsed value would be out by a factor of 100.
    off_scale <- data_long$bound[!is.na(data_long$bound) &
                                  (data_long$bound < 0 | data_long$bound > 1)]
    if (length(off_scale)) {
      stop("VT: ", length(off_scale), " censoring bound(s) fall outside [0, 1] ",
           "on the rate scale (first: ", off_scale[[1]], "). The workbook's ",
           "markers are assumed to be percent points; check the file.",
           call. = FALSE)
    }

    row_keys <- c("school_year", "unit", "public_independent", "grade",
                  "geography", "enrollment")

    # The workbook's `Exemption` column carries exactly three categories --
    # "Medical Exemptions", "Religious Exemptions" and "Provisional
    # Admittance" -- and each is kept as its own column rather than being
    # folded into one exemption total.
    # `measure` is the bare suffix, not a full column name: each one is pivoted
    # out twice below, as pct_<measure> and flag_<measure>.
    data_typed <- data_long %>%
      mutate(
        measure = case_when(
          type == "Exemption" & str_detect(exemption, "Medical") ~ "medical_exempt",
          type == "Exemption" & str_detect(exemption, "Religious") ~ "religious_exempt",
          type == "Exemption" & str_detect(exemption, "Provision") ~ "provisional_admittance",
          type == "Immunization" & str_detect(immunization, "DTaP") ~ "dtap",
          type == "Immunization" & str_detect(immunization, "Polio") ~ "polio",
          type == "Immunization" & str_detect(immunization, "MMR") ~ "mmr",
          type == "Immunization" & str_detect(immunization, "HepB") ~ "hep_b",
          type == "Immunization" & str_detect(immunization, "Varicella") ~ "varicella",
          type == "Immunization" & str_detect(immunization, "Full") ~ "fully_immunized",
          TRUE ~ NA_character_
        )
      ) %>%
      filter(!is.na(measure))

    # Vermont repealed its philosophical exemption in 2016, so religious is the
    # only non-medical category the workbook reports. The cross-state
    # pct_personal_exempt column is therefore a copy of the religious series
    # rather than a sum over several categories -- both are emitted so a
    # Vermont-specific read gets rate_religious_exempt and the harmonized
    # all-states build still finds rate_personal_exempt.
    data_typed <- bind_rows(
      data_typed,
      data_typed %>%
        filter(measure == "religious_exempt") %>%
        mutate(measure = "personal_exempt")
    )

    data_measures <- data_typed %>%
      group_by(across(all_of(c(row_keys, "measure")))) %>%
      summarize(
        value = if (all(is.na(value))) NA_real_ else max(value, na.rm = TRUE),
        flag = combine_censor_flag(flag),
        .groups = "drop"
      )

    # One column per measure for the value and one for its flag. These replace
    # the row-level suppressed_flag and censor_direction this file used to
    # carry: those said only that SOMETHING on the row was censored, which the
    # per-measure flags say precisely, so the rollup added nothing.
    #
    # scripts/build_all_states_county_standard.R still reads suppressed_flag and
    # censor_direction, so Vermont now contributes empty values for those two
    # columns of the all-states file. Its censoring information lives in
    # flag_<measure> here.
    values_wide <- data_measures %>%
      transmute(across(all_of(row_keys)), name = paste0("pct_", measure), value) %>%
      pivot_wider(names_from = name, values_from = value)

    flags_wide <- data_measures %>%
      transmute(across(all_of(row_keys)), name = paste0("flag_", measure), flag) %>%
      pivot_wider(names_from = name, values_from = flag)

    data_wide <- values_wide %>%
      left_join(flags_wide, by = row_keys)

    data_out <- data_wide %>%
      mutate(
        end_year = school_year_end_from_label(school_year),
        time = as.Date(school_year_time_from_end(end_year)),
        area = geography
      ) %>%
      select(-geography) %>%
      # Rows whose unit of analysis is not "County" are the statewide series.
      # A row that claims to be a county but does not resolve is now an error
      # rather than a silent addition to the statewide total.
      join_county_fips(
        "VT",
        county_col = "area",
        statewide = c("Vermont", "Statewide", "State", "Total")
      ) %>%
      mutate(
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
        time, geography, geography_name, grade, unit, public_independent,
        # The denominator sits with the row identifiers rather than among the
        # measures: every rate on the row is a share of it.
        enrollment,
        N_dtap, N_polio, N_mmr, N_hep_b, N_varicella,
        N_personal_exempt, N_medical_exempt, N_full_exempt,
        pct_dtap, pct_polio, pct_mmr, pct_hep_b, pct_varicella,
        pct_personal_exempt, pct_medical_exempt, pct_religious_exempt,
        pct_full_exempt,
        pct_fully_immunized, pct_provisional_admittance,
        # Same order as the pct_ block above, so each rate lines up with its
        # flag by position as well as by name.
        flag_dtap, flag_polio, flag_mmr, flag_hep_b, flag_varicella,
        flag_personal_exempt, flag_medical_exempt, flag_religious_exempt,
        flag_fully_immunized, flag_provisional_admittance
      )

    dir.create("standard", showWarnings = FALSE)
    write_standard(data_out, "Vermont", "standard/data.csv.gz", from = "rate")
  }

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
