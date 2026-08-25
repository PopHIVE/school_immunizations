library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/county_fips.R")

# "~$..." files are Excel's lock files, present only while a workbook is open
# in Excel: they are not data, are not readable as xlsx, and hashing them made
# the raw state depend on whether someone had a file open.
raw_workbooks <- function() {
  files <- list.files("raw", pattern = "\\.xlsx$", recursive = TRUE,
                       full.names = TRUE)
  files[!grepl("^~\\$", basename(files))]
}
raw_state <- as.list(tools::md5sum(raw_workbooks()))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw_files <- raw_workbooks()

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
  # Wisconsin out rather than averaging them.
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
  data_out <- data_all %>%
    join_county_fips("WI") %>%
    transmute(
      time, geography, geography_name,
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

  write_standard(data_out, "Wisconsin", "./standard/data.csv.gz", from = "rate")
  
  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
