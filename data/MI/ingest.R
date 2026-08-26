source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
# =============================================================================
# MI - Immunization Status by Building (Multiple Cohorts)
# =============================================================================

library(dplyr)
library(readxl)
library(stringr)
library(vroom)

if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

script_hash <- as.character(tools::md5sum("ingest.R"))

# All building-level workbooks share one layout (title block, then a header row
# of NAME/DISTRICT/TYPE/COUNTY/... at row 8). Filenames are inconsistent across
# years -- 2019-2021 use "Kind_2019_For Website.xlsx" / "7th_2021.xlsx" while
# 2022+ use "... Immunization Status by Building ...", so match on extension and
# exclude the one workbook that is not building-level:
#   "Waiver data by county 2019 - 2023.xlsx" is county x waiver-rate for a
#   combined 2019-2023 period, with no school or year detail.
raw_files <- list.files("raw", pattern = "\\.xlsx$", full.names = TRUE)
raw_files <- raw_files[!grepl("Waiver data by county", basename(raw_files))]
raw_state <- list(hash = tools::md5sum(raw_files))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  # Bare county names, matching what join_county_fips() emits for every other
  # state, so geography_name reads "Wayne" here and not "Wayne County".
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "MI") %>%
    mutate(geography_name = sub(" County$", "", geography_name)) %>%
    select(geography, geography_name, state)

  # Matched most-specific first: "kind" covers Kindergarten*, Kind_2019 and
  # Kindergarten_2021; "7th" covers 7th_2019 as well as "Seventh Graders".
  detect_grade <- function(filename) {
    fname <- tolower(filename)
    if (str_detect(fname, "new entrants")) return("New Entrants")
    if (str_detect(fname, "all grades")) return("All Grades")
    if (str_detect(fname, "kind")) return("Kindergarten")
    if (str_detect(fname, "seventh|7th")) return("7th Grade")
    "Unknown"
  }

  detect_end_year <- function(filename) {
    m <- str_match(filename, "(20\\d{2})")
    if (is.na(m[1, 1])) return(NA_integer_)
    as.integer(m[1, 1])
  }

  # MDHHS county labels that do not match a Michigan FIPS name.
  #   "Detroit"      - reported separately from Wayne, but the city lies wholly
  #                    within Wayne County, so its schools fold into Wayne.
  #   "Gd. Traverse" - abbreviation of Grand Traverse.
  # "No County Affiliation" is left alone: it is not a county and is dropped.
  normalize_county <- function(x) {
    x <- str_trim(x)
    dplyr::recode(x, "Detroit" = "Wayne", "Gd. Traverse" = "Grand Traverse")
  }

  process_file <- function(path) {
    grade <- detect_grade(basename(path))
    end_year <- detect_end_year(basename(path))
    time <- school_year_time_from_end(end_year)

    d <- read_excel(path, skip = 7)
    d <- d %>% filter(!is.na(NAME), !is.na(COUNTY))

    d %>%
      transmute(
        time = time,
        geography_name = str_to_title(normalize_county(COUNTY)),
        school_name = NAME,
        district = DISTRICT,
        school_type = TYPE,
        grade = grade,
        # MDHHS reports waivers; emit them under the repo's standard exemption
        # names so this file needs no downstream renaming. Column order in the
        # workbooks is: total, medical, religious, philosophical.
        N_enrolled = N,
        N_complete = COMP,
        pct_complete = `%COMP`,
        N_provisional = PROV,
        N_incomplete = INCOM,
        N_full_exempt = `n...10`,
        pct_full_exempt = `%...11`,
        N_medical_exempt = `n...12`,
        pct_medical_exempt = `%...13`,
        N_religious_exempt = `n...14`,
        pct_religious_exempt = `%...15`,
        N_personal_exempt = `n...16`,
        pct_personal_exempt = `%...17`
      ) %>%
      mutate(
        across(
          c(
            N_enrolled,
            N_complete,
            pct_complete,
            N_provisional,
            N_incomplete,
            N_full_exempt,
            pct_full_exempt,
            N_medical_exempt,
            pct_medical_exempt,
            N_religious_exempt,
            pct_religious_exempt,
            N_personal_exempt,
            pct_personal_exempt
          ),
          ~ suppressWarnings(as.numeric(.x))
        )
      )
  }

  unknown <- basename(raw_files)[vapply(basename(raw_files), detect_grade, "") == "Unknown"]
  if (length(unknown)) {
    warning("no grade detected for: ", paste(unknown, collapse = ", "), call. = FALSE)
  }

  schools <- bind_rows(lapply(raw_files, process_file)) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name"))

  # Report counties that failed the FIPS join instead of dropping them silently.
  unmatched <- sort(unique(schools$geography_name[is.na(schools$geography)]))
  if (length(unmatched)) {
    warning(length(unmatched), " county name(s) matched no MI FIPS and were dropped: ",
            paste(unmatched, collapse = ", "), call. = FALSE)
  }
  schools <- schools %>%
    filter(!is.na(geography), state == "MI") %>%
    select(-state) %>%
    mutate(type = "school")

  # County totals, summed across every building in the county at that grade and
  # school year. MDHHS publishes only building-level counts, so the county row is
  # derived rather than reported: its shares are recomputed from the summed
  # counts instead of averaging the buildings' published percentages, which would
  # weight a 20-pupil school the same as a 2,000-pupil one.
  #
  # Percent points, not proportions, because write_standard(from = "percent")
  # applies one scale to every pct_ column in the frame.
  COUNT_COLS <- c("N_enrolled", "N_complete", "N_provisional", "N_incomplete",
                  "N_full_exempt", "N_medical_exempt", "N_religious_exempt",
                  "N_personal_exempt")

  counties <- schools %>%
    group_by(time, geography_name, geography, grade) %>%
    summarize(across(all_of(COUNT_COLS), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
    mutate(
      type = "county",
      school_name = NA_character_,
      district = NA_character_,
      school_type = NA_character_,
      pct_complete = 100 * rate_from_counts(N_complete, N_enrolled),
      pct_full_exempt = 100 * rate_from_counts(N_full_exempt, N_enrolled),
      pct_medical_exempt = 100 * rate_from_counts(N_medical_exempt, N_enrolled),
      pct_religious_exempt = 100 * rate_from_counts(N_religious_exempt, N_enrolled),
      pct_personal_exempt = 100 * rate_from_counts(N_personal_exempt, N_enrolled)
    )

  data <- bind_rows(schools, counties) %>%
    select(
      time, geography_name, geography, type,
      school_name, district, school_type, grade,
      everything()
    )

  message(sprintf(
    "MI: %d rows from %d workbooks (%d school, %d county), school years %s",
    nrow(data), length(raw_files),
    sum(data$type == "school"), sum(data$type == "county"),
    paste(sort(unique(data$time)), collapse = ", ")))

  dir.create("standard", showWarnings = FALSE)
  write_standard(data, "Michigan", "standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
