library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")

# =============================================================================
# WI - three sources, two of them fetched here, one committed by hand.
#
#   dhs_school_workbooks  raw/School Immunization Rates, Wisconsin, <year>.xlsx
#     The per-year "By School" workbooks from the DHS P-01892 collection. These
#     are not downloaded by this script (the collection page has no stable
#     per-year link) and are the school rows of standard/data.csv.gz.
#
#   wir_mmr_county        raw/mmr-map-data.xlsx
#     County MMR coverage from the Wisconsin Immunization Registry (WIR), by
#     CALENDAR year, from the DHS child and adolescent vaccine data page. Its
#     "Read me" sheet is explicit that this is registry data, "distinct from
#     the school vaccination assessments": a different population, a different
#     denominator and a different clock from the school survey, so the three
#     measures are named for what they are (rate_mmr_1dose_24m,
#     rate_mmr_2dose_6y, rate_mmr_2dose_6_18y) rather than folded into the
#     school-survey rate_mmr. These are the county rows (type = "county").
#
#   dhs_arcgis_schools    raw/wi_arcgis_schools.csv, raw/wi_arcgis_districts.csv
#     The school and district layers behind the DHS school immunization web
#     map. Downloaded as a snapshot only; see the note at the fetch for why no
#     rows are taken from it.
# =============================================================================

sources <- read_sources()
process <- dcf::dcf_process_record()
prev <- process$fetch_state
dir.create("raw", showWarnings = FALSE)

# ---- WIR county MMR workbook -------------------------------------------------
#
# sources.json carries the URL the file was first found under
# (mmr-map-data-2019-2024.xlsx). The vaccine data page itself links the same
# bytes as mmr-map-data.xlsx with no year span, and DHS revises every year in
# the workbook in place (the "Read me" sheet says all years were restated in
# 2025 when the age bands changed), so the file is always re-requested
# (if_exists = "replace") and kept under one fixed name in raw/ rather than
# under whatever name the server used. A renamed file (say
# mmr-map-data-2019-2025.xlsx) is picked up by discovery on the page; the
# newest year in the name wins, a name with no year comes next, and the
# sources.json URL is the fallback when the page cannot be read (raw/ is
# committed, so discovery failing is a warning, not a stop).
mmr_src <- source_entry(sources, "wir_mmr_county")
mmr_path <- "raw/mmr-map-data.xlsx"

posted <- discover_links(mmr_src$page_url, mmr_src$pattern, must_find = FALSE)
mmr_url <- mmr_src$url
if (nrow(posted)) {
  yr <- suppressWarnings(as.integer(str_extract(basename(posted$url), "(\\d{4})(?=\\.xlsx$)")))
  ord <- order(is.na(yr), -ifelse(is.na(yr), 0L, yr))
  mmr_url <- posted$url[ord][1]
  if (!identical(mmr_url, mmr_src$url)) {
    message("WI: MMR workbook taken from the vaccine data page: ", mmr_url)
  }
}

mmr_rec <- fetch_file(
  mmr_url, mmr_path, type = "xlsx", previous = prev[[mmr_path]],
  validate = function(path) {
    if (!"Data" %in% readxl::excel_sheets(path)) stop("no 'Data' sheet")
  }
)

# ---- DHS ArcGIS school / district layers -------------------------------------
#
# The map service describes itself as "school year, 2019/2020", but that text
# is stale: matched on the district/school code (DIST_SCHL against the code
# DHS appends to School Name from the 2022 workbook on), the school layer
# agrees with "School Immunization Rates, Wisconsin, 2022.xlsx" (the 2022-23
# school year) on "% Met Minimum Requirements" for 2,060 of 2,066 schools
# that reported, and with no other workbook. The same year is already in
# raw/ as a workbook, so no rows are taken from the layer; it is fetched and
# kept as a snapshot so a later refresh of the service shows up in
# fetch_state and in the year check below. The layer also has no county
# column (only DISTRICT and CITY), which is the other reason its rows cannot
# join the school rows as they are.
#
# arcgis_layer_csv() stops on any HTTP error and has no committed-copy
# fallback of its own, so it is wrapped here: a failed refresh is a warning
# and a "failed" record, and the ingest continues on the committed snapshot.
arc_src <- source_entry(sources, "dhs_arcgis_schools")
arc_paths <- c(schools = "raw/wi_arcgis_schools.csv",
               districts = "raw/wi_arcgis_districts.csv")

fetch_arcgis_layer <- function(layer, dest) {
  tryCatch(
    arcgis_layer_csv(arc_src$url, layer, dest),
    error = function(e) {
      base <- sprintf("%s/%d/query", sub("/+$", "", arc_src$url), as.integer(layer))
      if (file.exists(dest)) {
        warning(sprintf("fetch: %s could not be refreshed from %s (%s); keeping the committed copy",
                        basename(dest), base, conditionMessage(e)), call. = FALSE)
        return(fetch_record(base, dest, "failed", bytes = file.size(dest),
                            sha256 = fetch_sha256(dest), attempts = 1L,
                            error = conditionMessage(e)))
      }
      warning(sprintf("fetch: %s could not be downloaded from %s (%s) and there is no committed copy",
                      basename(dest), base, conditionMessage(e)), call. = FALSE)
      NULL
    }
  )
}
arc_recs <- list(fetch_arcgis_layer(0L, arc_paths[["schools"]]),
                 fetch_arcgis_layer(1L, arc_paths[["districts"]]))

process <- record_fetch(process, c(list(mmr_rec), arc_recs))

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # "~$..." files are Excel's lock files, present only while a workbook is open
  # in Excel: they are not data, are not readable as xlsx, and hashing them made
  # the raw state depend on whether someone had a file open. raw_state_md5()
  # above excludes them the same way.
  raw_files <- list.files("raw", pattern = "^School Immunization Rates.*\\.xlsx$",
                          full.names = TRUE)
  raw_files <- raw_files[!grepl("^~\\$", basename(raw_files))]

  # DHS spells the personal-conviction column two ways across the workbooks
  # ("Waiver" in some years, "Wavier" in others). Matching only one spelling
  # dropped that measure for the years using the other, silently.
  first_present <- function(data, names_wanted) {
    hit <- intersect(names_wanted, names(data))
    if (!length(hit)) return(rep(NA_real_, nrow(data)))
    data[[hit[1]]]
  }

  # DHS suppresses small school-level shares as "<5" and top-codes as ">95",
  # and separately marks a school as never having reported via the Comment
  # column rather than a value in the waiver cell itself. Per-request, "<5" is
  # recoded to 4 and ">95" to 96 -- a value on the right side of the printed
  # bound, so the row keeps a number instead of going NA -- and each of the
  # four waiver measures gets its own flag column recording which happened,
  # since a school can be suppressed on one waiver and reported on another.
  recode_extreme <- function(x, comment) {
    chr <- trimws(as.character(x))
    cmt <- trimws(as.character(comment))
    no_report <- !is.na(cmt) & cmt == "No report received"
    bottom <- !is.na(chr) & grepl("^<\\s*5\\s*%?$", chr)
    top <- !is.na(chr) & grepl("^>\\s*95\\s*%?$", chr)

    flag <- rep(NA_character_, length(chr))
    flag[bottom] <- "bottom_coded"
    flag[top] <- "top_coded"
    flag[no_report] <- "no_report_received"

    value <- chr
    value[bottom] <- "4"
    value[top] <- "96"
    value[no_report] <- NA_character_

    list(value = value, flag = flag)
  }

  data_all <- bind_rows(lapply(raw_files, function(path) {
    year_match <- str_extract(basename(path), "\\d{4}")
    time <- as.Date(paste0(year_match, "-09-01"))

    data_raw <- readxl::read_excel(path, sheet = "By School")
    comment <- first_present(data_raw, "Comment")

    medical <- recode_extreme(
      first_present(data_raw, "% Health Waiver"), comment)
    religious <- recode_extreme(
      first_present(data_raw, "% Religious Waiver"), comment)
    personal <- recode_extreme(
      first_present(data_raw, c("% Personal Conviction Waiver",
                                 "% Personal Conviction Wavier")),
      comment)
    # The overall exemption measure, which was going unread.
    full <- recode_extreme(
      first_present(data_raw, "% Waived All Vaccines"), comment)

    data_raw %>%
      transmute(
        county = County,
        city = if ("City" %in% names(data_raw)) City else NA_character_,
        school_name = if ("School Name" %in% names(data_raw)) `School Name`
                      else NA_character_,
        pct_medical_exempt = parse_rate(medical$value, from = "percent"),
        flag_medical_exempt = medical$flag,
        pct_religious_exempt = parse_rate(religious$value, from = "percent"),
        flag_religious_exempt = religious$flag,
        pct_personal_exempt = parse_rate(personal$value, from = "percent"),
        flag_personal_exempt = personal$flag,
        pct_full_exempt = parse_rate(full$value, from = "percent"),
        flag_full_exempt = full$flag,
        time = time
      )
  }))

  # Rows are kept AT SCHOOL LEVEL, the granularity DHS publishes.
  #
  # This file used to report a county figure taken as an UNWEIGHTED mean of the
  # school percentages, which is not a county rate: DHS publishes shares with no
  # enrolment anywhere in the workbook, so a 12-pupil school and a 2,000-pupil
  # school counted equally, and small cells are suppressed as "<5", which leaves
  # the average resting on whichever few schools printed a number. Shawano County
  # 2020-21 came out at a 0.95 religious-waiver rate off zero schools reporting a
  # medical figure. Without a denominator there is no defensible way to roll these
  # up, so the school rows are published as they are and the county file leaves
  # Wisconsin out rather than averaging them. The county rows added below are
  # NOT that roll-up: they are registry MMR coverage from a different source.
  #
  # The county FIPS is still resolved per school: the 2018 and 2019 workbooks
  # spell Walworth "Walwroth" on some rows and correctly on others, and use
  # "Saint Croix" for St. Croix.
  # Per-measure flag_<measure> columns replace the row-level suppressed_flag
  # and censor_direction this file used to carry: those said only that
  # SOMETHING on the row was censored, not which of the four waivers, or
  # whether it was bottom/top-coded versus never reported.
  # scripts/build_all_states_county_standard.R still reads suppressed_flag
  # and censor_direction, so Wisconsin now contributes empty values for
  # those two columns of the all-states file.
  schools <- data_all %>%
    join_county_fips("WI") %>%
    transmute(
      time, geography, geography_name,
      type = "school",
      school_name, city,
      grade = "Overall",
      pct_medical_exempt, flag_medical_exempt,
      pct_religious_exempt, flag_religious_exempt,
      pct_personal_exempt, flag_personal_exempt,
      # Religious and personal-conviction waivers are kept apart rather than
      # summed: adding them produced rates above 100% (Juneau County reached
      # 133% in 2022-23). "% Waived All Vaccines" above is the source's own
      # overall figure, so nothing has to be added up here.
      pct_full_exempt, flag_full_exempt
    ) %>%
    arrange(time, geography, school_name)

  # ---- county MMR coverage from the registry ---------------------------------
  #
  # The "Data" sheet is one row per County x Year: Statewide plus the 72
  # counties, for 2019 onward, with three percent-point columns. Points to
  # know from the "Read me" sheet, none of which this script adjusts for:
  #
  #   * The Year cell carries a footnote mark in some years ("2023" with a
  #     dagger). It marks a denominator change: from 2023 on, WIR records
  #     for people aged 11+ with no update in 10 years and no query in 5 are
  #     dropped as probably no longer resident, so the 6-18 series from 2023
  #     is not comparable with earlier years. The mark is stripped to get the
  #     year; the break is documented here and in sources.json, not flagged
  #     per row.
  #   * In 2025 the age bands changed from 5-6 / 5-18 to 6 / 6-18 year olds,
  #     and DHS restated every year in the workbook on the new bands. That is
  #     why the whole file is re-fetched rather than one year at a time.
  #   * Coverage is by CALENDAR year. The standard `time` is the September a
  #     school year started, so the calendar year is taken as the year the
  #     school year ENDED -- 2023 is dated 2022-09-01 -- the mapping NM uses
  #     for its calendar-year registry data. A calendar year overlaps the
  #     tail of one school year and the head of the next, so this is a
  #     recorded decision rather than something the source states.
  #
  # Columns are matched on their wording rather than position, and a missing
  # match stops the build: the column headers use an en dash ("6-18"), which
  # is why the age-band patterns below allow any single character there.
  mmr_raw <- readxl::read_excel(mmr_path, sheet = "Data")
  mmr_col <- function(pattern) {
    hit <- grep(pattern, names(mmr_raw), ignore.case = TRUE, perl = TRUE, value = TRUE)
    if (length(hit) != 1) {
      stop("WI: expected one MMR column matching /", pattern, "/, found ",
           length(hit), " in: ", paste(names(mmr_raw), collapse = " | "))
    }
    mmr_raw[[hit]]
  }
  mmr_year <- str_extract(as.character(mmr_raw$Year), "^\\d{4}")
  if (anyNA(mmr_year)) {
    stop("WI: MMR Year values that do not start with a 4-digit year: ",
         paste(unique(mmr_raw$Year[is.na(mmr_year)]), collapse = ", "))
  }

  counties <- tibble(
    county = trimws(as.character(mmr_raw$County)),
    year = as.integer(mmr_year),
    pct_mmr_1dose_24m = parse_rate(mmr_col("^percent 24 month olds with 1 dose"), from = "percent"),
    pct_mmr_2dose_6y = parse_rate(mmr_col("^percent 6 year olds with 2 doses"), from = "percent"),
    pct_mmr_2dose_6_18y = parse_rate(mmr_col("^percent 6.18 year olds with 2 doses"), from = "percent")
  ) %>%
    filter(!is.na(county), county != "") %>%
    join_county_fips("WI", statewide = "Statewide") %>%
    transmute(
      time = as.Date(school_year_time_from_end(year)),
      geography, geography_name,
      type = "county",
      school_name = NA_character_, city = NA_character_,
      grade = "Overall",
      pct_mmr_1dose_24m, pct_mmr_2dose_6y, pct_mmr_2dose_6_18y
    )

  dup <- counties %>% count(time, geography) %>% filter(n > 1)
  if (nrow(dup)) {
    stop("WI: MMR sheet has more than one row for ", nrow(dup),
         " county-year(s), e.g. ", dup$geography[1], " ", dup$time[1])
  }

  # ---- which school year the ArcGIS snapshot carries ------------------------
  #
  # Reported, not acted on. The service's own description cannot be trusted
  # for the year (see the fetch above), so the snapshot is matched against
  # each workbook whose School Name carries the "dddd/dddd" district/school
  # code. If none agrees, DHS has moved the service on to a year with no
  # workbook in raw/, and that year needs a reader (and a county lookup for
  # its schools) before its rows can be published.
  arc_year <- NA_integer_
  if (file.exists(arc_paths[["schools"]])) {
    arc <- vroom::vroom(arc_paths[["schools"]], show_col_types = FALSE, progress = FALSE)
    arc <- arc %>%
      filter(COMMENT == "Report received") %>%
      transmute(code = DIST_SCHL, arc_min = sub("%$", "", trimws(WebAllPercentMinReq_TXT)))
    for (path in raw_files) {
      wb <- readxl::read_excel(path, sheet = "By School")
      wb <- wb %>%
        filter(Comment == "Report received") %>%
        transmute(code = str_extract(`School Name`, "\\d{4}/\\d{4}$"),
                  wb_min = trimws(as.character(`% Met Minimum Requirements`))) %>%
        filter(!is.na(code))
      if (!nrow(wb)) next
      m <- inner_join(wb, arc, by = "code")
      if (nrow(m) >= 500 && mean(m$wb_min == m$arc_min, na.rm = TRUE) >= 0.95) {
        arc_year <- as.integer(str_extract(basename(path), "\\d{4}"))
        break
      }
    }
    if (is.na(arc_year)) {
      message("WI: the ArcGIS school layer matches none of the committed workbooks; ",
              "DHS may have moved it to a new school year. It is not published from here.")
    } else {
      message(sprintf("WI: ArcGIS school layer matches the %d-%02d workbook; snapshot only",
                      arc_year, (arc_year + 1) %% 100))
    }
  }

  # School rows and county rows in one file, told apart by `type`, the way HI,
  # MD and OR do it. The school rows carry the waiver measures and the county
  # rows the three MMR measures; each is empty on the other's rows.
  data_out <- bind_rows(schools, counties) %>%
    select(time, geography, geography_name, type, school_name, city, grade,
           everything()) %>%
    arrange(time, geography, type, school_name)

  message(sprintf(
    "WI: %d rows (%d school from %d workbooks, %d county MMR for %d years)",
    nrow(data_out), sum(data_out$type == "school"), length(raw_files),
    sum(data_out$type == "county"), n_distinct(counties$time)))

  out <- write_standard(data_out, "Wisconsin", "./standard/data.csv.gz", from = "rate")

  update_latest_year(latest_school_year(out[out$type == "school", ]),
                     ids = "dhs_school_workbooks")
  update_latest_year(latest_school_year(out[out$type == "county", ]),
                     ids = "wir_mmr_county")
  if (!is.na(arc_year)) update_latest_year(arc_year, ids = "dhs_arcgis_schools")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
