source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")
# =============================================================================
# MI - Immunization Status by Building (Multiple Cohorts)
# =============================================================================

library(dcf)
library(dplyr)
library(readxl)
library(stringr)
library(vroom)

sources <- read_sources()
src <- source_entry(sources, "mdhhs_building_files")

if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}
prev <- process$fetch_state

# MDHHS posts one building-level workbook per cohort per year on its school
# immunization data page, and links only the current year there: as of
# September 2026 the page carries the 2025 Kindergarten, Seventh-Grade and
# New-Entrants files. The media hrefs carry ?rev=&hash= query strings that
# change when the file is re-uploaded, so the query is stripped when naming
# the copy in raw/. Each year's workbook is final once posted (the 2024
# "PROVISIONAL All Grades" workbook in raw/ was the exception, and it was
# hand-downloaded), so files already on disk are not re-requested.
#
# michigan.gov intermittently blocks non-browser clients; when the index
# page cannot be read the ingest continues on what is committed in raw/.
dir.create("raw", showWarnings = FALSE)
dest_from_url <- function(u) file.path("raw", basename(sub("\\?.*$", "", u)))

links <- discover_links(src$page_url, src$pattern, must_find = FALSE)
recs <- list()
if (nrow(links)) {
  recs <- fetch_many(links$url, dest_fn = dest_from_url, type = "xlsx",
                     if_exists = "skip", previous = prev)
}

# The 2024 files are no longer linked from the page, but the Kindergarten and
# New-Entrants workbooks still exist at the same media path (verified
# 2026-09-04; Seventh-Grade-...-2024.xlsx returns 404, so it is not listed).
# These are one attempt each with no committed copy to fall back on, so a
# miss is reported and the ingest goes on without the file.
mdhhs_media <- paste0(
  "https://www.michigan.gov/mdhhs/-/media/Project/Websites/mdhhs/",
  "Adult-and-Childrens-Services/Children-and-Families/Immunization-Information/",
  "School-Waiver-Information"
)
unlisted <- c(
  "Kindergarten-Immunization-Status-by-Building-2024.xlsx",
  "New-Entrants-Immunization-Status-by-Building-2024.xlsx"
)
for (f in unlisted) {
  dest <- file.path("raw", f)
  rec <- tryCatch(
    fetch_file(paste0(mdhhs_media, "/", f), dest, type = "xlsx",
               if_exists = "skip", retries = 1L, previous = prev[[dest]]),
    error = function(e) {
      message("MI: ", conditionMessage(e))
      NULL
    }
  )
  if (!is.null(rec)) recs <- c(recs, list(rec))
}
process <- record_fetch(process, recs)

script_hash <- as.character(tools::md5sum("ingest.R"))
raw_state <- raw_state_md5()

# All building-level workbooks share one layout (title block, then a header row
# of NAME/DISTRICT/TYPE/COUNTY/... at row 8). Filenames are inconsistent across
# years -- 2019-2021 use "Kind_2019_For Website.xlsx" / "7th_2021.xlsx",
# 2022-2023 "... Immunization Status by Building ...", and the downloads from
# 2024 on "Kindergarten-Immunization-Status-by-Building-2025.xlsx" -- so match
# on extension and exclude the one workbook that is not building-level:
#   "Waiver data by county 2019 - 2023.xlsx" is county x waiver-rate for a
#   combined 2019-2023 period, with no school or year detail.
#
# A PROVISIONAL workbook is superseded by the final one for the same end
# year. "PROVISIONAL All Grades Immunization Status by Building 2024.xlsx"
# holds one sheet per cohort (Kindergarten, Seventh, New Entrants,
# Childcare); read from its first sheet it duplicated the final 2023-24
# kindergarten rows under a grade of "All Grades". It stays in raw/ as a
# record, and is parsed only if no final workbook for its end year is on
# disk. The final Kindergarten and New-Entrants 2024 files are, so it is
# not.
#
# Seventh-Grade-Immunization-Status-by-Building-2024.xlsx returns 404 on
# the MDHHS media path (checked 2026-09-04), so 2023-24 has no seventh-grade
# rows. scripts/check_sources.R reports a newly linked file the pattern in
# sources.json matches, so it will show up there if MDHHS posts it.
raw_files <- list.files("raw", pattern = "\\.xlsx$", full.names = TRUE)
raw_files <- raw_files[!grepl("Waiver data by county", basename(raw_files))]
file_end_year <- as.integer(str_match(basename(raw_files), "(20\\d{2})")[, 2])
is_provisional <- grepl("PROVISIONAL", basename(raw_files), ignore.case = TRUE)
superseded <- is_provisional & file_end_year %in% file_end_year[!is_provisional]
if (any(superseded)) {
  message("MI: not parsing provisional workbook(s) superseded by a final one: ",
          paste(basename(raw_files)[superseded], collapse = ", "))
}
raw_files <- raw_files[!superseded]

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  # Matched most-specific first: "kind" covers Kindergarten*, Kind_2019 and
  # Kindergarten_2021; "7th" covers 7th_2019 as well as "Seventh Graders".
  # "New Entrants" is spelled with a space in the hand-downloaded 2023 file
  # and with a hyphen in the files fetched from the media path.
  detect_grade <- function(filename) {
    fname <- tolower(filename)
    if (str_detect(fname, "new[ _-]entrants")) return("New Entrants")
    if (str_detect(fname, "all grades")) return("All Grades")
    if (str_detect(fname, "kind")) return("Kindergarten")
    if (str_detect(fname, "seventh|7th")) return("7th Grade")
    "Unknown"
  }

  # The year in the filename is the one in the workbook's own title ("2023
  # IMMUNIZATION STATUS OF KINDERGARTEN STUDENTS"), and it is read as the END
  # of the school year: a file named 2025 is school year 2024-25 and is dated
  # 2024-09-01. Every year in raw/ is dated the same way.
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
        county = normalize_county(COUNTY),
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

  # Any county label other than the two recoded above and the non-county
  # "No County Affiliation" stops the build here rather than being dropped.
  schools <- bind_rows(lapply(raw_files, process_file)) %>%
    join_county_fips("MI", drop = "^No County Affiliation$") %>%
    select(-county) %>%
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
  out <- write_standard(data, "Michigan", "standard/data.csv.gz", from = "percent")
  update_latest_year(latest_school_year(out))

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
