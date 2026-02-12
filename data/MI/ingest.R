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

raw_files <- list.files("raw", pattern = "Immunization Status", full.names = TRUE)
raw_state <- list(hash = tools::md5sum(raw_files))

if (!identical(process$raw_state, raw_state)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "MI") %>%
    select(geography, geography_name, state)

  detect_grade <- function(filename) {
    fname <- tolower(filename)
    if (str_detect(fname, "kindergarten")) return("Kindergarten")
    if (str_detect(fname, "seventh")) return("7th Grade")
    if (str_detect(fname, "new entrants")) return("New Entrants")
    if (str_detect(fname, "all grades")) return("All Grades")
    "Unknown"
  }

  detect_end_year <- function(filename) {
    m <- str_match(filename, "(20\\d{2})")
    if (is.na(m[1, 1])) return(NA_integer_)
    as.integer(m[1, 1])
  }

  process_file <- function(path) {
    grade <- detect_grade(basename(path))
    end_year <- detect_end_year(basename(path))
    time <- format(as.Date(paste0(end_year, "-12-31")), "%m-%d-%Y")

    d <- read_excel(path, skip = 7)
    d <- d %>% filter(!is.na(NAME), !is.na(COUNTY))

    d %>%
      transmute(
        time = time,
        county = COUNTY,
        geography_name = paste0(str_to_title(str_trim(COUNTY)), " County"),
        school_name = NAME,
        district = DISTRICT,
        school_type = TYPE,
        grade = grade,
        n_students = N,
        n_complete = COMP,
        pct_complete = `%COMP`,
        n_provisional = PROV,
        n_incomplete = INCOM,
        n_waiver_total = `n...10`,
        pct_waiver_total = `%...11`,
        n_waiver_medical = `n...12`,
        pct_waiver_medical = `%...13`,
        n_waiver_religious = `n...14`,
        pct_waiver_religious = `%...15`,
        n_waiver_philosophical = `n...16`,
        pct_waiver_philosophical = `%...17`
      ) %>%
      mutate(
        across(
          c(
            n_students,
            n_complete,
            pct_complete,
            n_provisional,
            n_incomplete,
            n_waiver_total,
            pct_waiver_total,
            n_waiver_medical,
            pct_waiver_medical,
            n_waiver_religious,
            pct_waiver_religious,
            n_waiver_philosophical,
            pct_waiver_philosophical
          ),
          ~ suppressWarnings(as.numeric(.x))
        )
      )
  }

  data <- bind_rows(lapply(raw_files, process_file)) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(state == "MI")

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
