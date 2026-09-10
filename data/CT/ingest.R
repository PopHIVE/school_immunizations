library(dcf)
library(dplyr)
library(tidyr)
library(readr)
library(readxl)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")

# =============================================================================
# CT - County/County-equivalent Immunization & Exemption Rates (county-level),
#      plus the by-school tables (school-level, standard/data_schools.csv.gz)
#
# Two county sources, stacked on the same (time, geography, grade) key:
#
#   1. CT Open Data (Socrata) dataset 8kid-pp5k, "County or County Equivalent
#      Immunizations and Exemption Rates by School Year, Grade, Vaccine, and
#      School Type". Pulled from the API so it self-updates. Covers 2012-13 ..
#      2025-26, grades Pre-K / K / 7th, and ten vaccine series -- one row per
#      year x county x grade x vaccine.
#   2. raw/CT Vaccine Exemptions 2017-2025_All Grades.xlsx -- DPH's ALL-GRADES
#      exemption rates by traditional county, 2017-18 .. 2024-25. This is the
#      only place CT publishes an all-grades figure, and it keeps traditional
#      counties for the 2022+ years where the Socrata extract switched to
#      planning regions. Manual download; there is no API for it.
#
# Every measure the sources publish per vaccine is emitted per vaccine, in the
# column name: pct_<vax>, pct_<vax>_religious_exempt, pct_<vax>_medical_exempt,
# pct_<vax>_non_compliant. The exemption percentages are usually constant
# across vaccines within a year x county x grade, but not always -- 2025-26
# Lower CT River Valley 7th grade reports a 1.2% medical exemption for HepA
# against 0.2% for every other series -- so collapsing them to one
# per-group value (as this ingest used to) loses real variation.
#
# Note: CT reports by traditional county for older years and by the new Council
#   of Governments "planning regions" (county-equivalents) from ~2022 on; both
#   are present in all_fips and handled by the crosswalk below.
# Note: the 2024-25 7th-grade STATEWIDE row is short -- 288 schools and 26,635
#   pupils against 383 schools and 36,679 across the nine planning regions. That
#   is how DPH published it, and it is a statewide row rather than a county one,
#   so it is carried as published; do not read it as a state total for the year.
#
# Output: standard/data.csv.gz stacks all four grades CT publishes, and is the
#   file scripts/build_all_states_county_standard.R and
#   scripts/generate_measure_info.R read by name -- it stays the canonical
#   pipeline output. But CT does not assess the same panel at every grade
#   (Pre-K reports only Flu; K has no Tdap or MenACWY; 7th's HepA only starts
#   2019-20; the All Grades workbook has no coverage or enrolment at all), so
#   every row in that stacked file carries several structurally-empty columns
#   for measures the OTHER grades have. Four narrower files sit beside it, one
#   per grade, each with that grade's own all-NA columns dropped:
#   standard/data_7th.csv.gz, data_k.csv.gz, data_pre_k.csv.gz and
#   data_all_grades.csv.gz.
#
# School-level: DPH also publishes three by-school tables on the same portal
#   (kindergarten coverage and exemptions, seventh-grade coverage and
#   exemptions, all-grades exemptions). They are parsed separately into
#   standard/data_schools.csv.gz -- see the school block at the end -- and do
#   not feed data.csv.gz or the per-grade files.
# =============================================================================

process <- dcf::dcf_process_record()
prev <- process$fetch_state

dir.create("raw", showWarnings = FALSE)
raw_file <- "raw/ct_county_immunization_exemption_8kid-pp5k.csv"
all_grades_file <- "raw/CT Vaccine Exemptions 2017-2025_All Grades.xlsx"

# The county extract used to be download.file()'d straight onto raw_file, and
# download.file() truncates its destination when the transfer fails, so a
# failed refresh destroyed the committed copy (the incident data/OR/ingest.R
# documents). socrata_csv() downloads to a temporary file, checks it is a CSV
# with the expected header and that it did not fill the $limit, and only then
# copies it over the committed one; a failure warns and leaves raw/ alone.
county_rec <- socrata_csv(
  "data.ct.gov", "8kid-pp5k", raw_file,
  expect_cols = c("school_year", "county_equivalent", "grade", "vaccine_series"),
  previous = prev[[raw_file]]
)
process <- record_fetch(process, county_rec)

# ---- By-school tables: discovery and download --------------------------------
# sources.json carries the three dataset ids, but the ingest does not trust
# them to stay put and finds the tables through the Socrata catalog instead.
# What it found in September 2026 is that the ids do NOT change: iux5-vrzq,
# rz57-x4bb and a2a4-pw6c were created on 2022-05-05 as the "2021-2022 ..."
# tables (the story page n5kk-6ext still links them under those titles), and
# each has since been retitled "2025-2026 ..." and overwritten in place. The
# portal keeps only the current year. So the year in the title is the only
# record of which survey a download holds, and each table is saved under it
# -- raw/ct_school_<k|7|exempt>_<start year>.csv -- so that the snapshots
# accumulate here across years even though the portal keeps one.
#
# A file for the newest year in the catalog is refreshed on every run (DPH
# can revise the current survey in place); a file for any earlier year is
# kept as is, since the portal no longer serves that year and a refresh
# would only ever fetch the wrong one.
#
# The catalog can be unreachable without blocking the ingest: raw/ is
# committed, and the parse below runs on whatever is on disk.
ct_catalog <- function(q) {
  url <- sprintf(
    "https://api.us.socrata.com/api/catalog/v1?domains=data.ct.gov&q=%s&limit=50",
    utils::URLencode(q, reserved = TRUE)
  )
  resp <- tryCatch(
    httr::GET(url, httr::add_headers(.headers = browser_headers(accept = "application/json")),
              httr::timeout(60)),
    error = function(e) e
  )
  if (inherits(resp, "error") || httr::status_code(resp) != 200L) {
    warning("CT: Socrata catalog query failed for '", q, "': ",
            if (inherits(resp, "error")) conditionMessage(resp) else paste("HTTP", httr::status_code(resp)),
            call. = FALSE)
    return(NULL)
  }
  js <- jsonlite::fromJSON(httr::content(resp, "text", encoding = "UTF-8"),
                           simplifyVector = FALSE)
  rows <- lapply(js$results, function(r) {
    data.frame(id = r$resource$id, name = r$resource$name,
               type = r$resource$type %||% NA_character_,
               stringsAsFactors = FALSE)
  })
  if (!length(rows)) return(NULL)
  do.call(rbind, rows)
}

catalog <- do.call(rbind, Filter(Negate(is.null), list(
  ct_catalog("immunization rates by school"),
  ct_catalog("vaccine exemption rates by school")
)))

school_kind <- function(name) {
  n <- tolower(name)
  dplyr::case_when(
    grepl("kindergarten", n) & grepl("immunization rates by school", n) ~ "k",
    grepl("seventh grade", n) & grepl("immunization rates by school", n) ~ "7",
    grepl("exemption rates by school", n) & grepl("all grades", n) ~ "exempt",
    TRUE ~ NA_character_
  )
}

school_recs <- list()
if (!is.null(catalog) && nrow(catalog)) {
  found <- catalog %>%
    filter(type == "dataset") %>%
    distinct(id, .keep_all = TRUE) %>%
    mutate(kind = school_kind(name),
           start_year = school_year_end_from_label(name) - 1L) %>%
    filter(!is.na(kind))
  untitled <- found %>% filter(is.na(start_year))
  if (nrow(untitled)) {
    warning("CT: by-school dataset(s) with no school year in the title, not fetched: ",
            paste(sprintf("%s (%s)", untitled$name, untitled$id), collapse = "; "),
            call. = FALSE)
  }
  found <- found %>%
    filter(!is.na(start_year)) %>%
    group_by(kind) %>%
    mutate(current = start_year == max(start_year)) %>%
    ungroup() %>%
    mutate(dest = sprintf("raw/ct_school_%s_%d.csv", kind, start_year))
  for (i in seq_len(nrow(found))) {
    school_recs[[found$dest[i]]] <- socrata_csv(
      "data.ct.gov", found$id[i], found$dest[i],
      expect_cols = c("school_name", "planning_region"),
      if_exists = if (found$current[i]) "replace" else "skip",
      previous = prev[[found$dest[i]]]
    )
    if (i < nrow(found)) Sys.sleep(2)
  }
  process <- record_fetch(process, school_recs)
}

# Every raw file is hashed -- the all-grades workbook is .xlsx, so a pattern
# matching only "csv" would leave it out of the gate and a replaced workbook
# would not trigger a reprocess.
raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw <- readr::read_csv(raw_file, show_col_types = FALSE,
                         col_types = readr::cols(.default = "c"))

  # The one value a column takes across a group, or an error if the group
  # genuinely disagrees. Used for total_enrollment_count, which the source
  # repeats on every vaccine row of a year x county x grade: taking the first
  # would hide a source change silently, and taking the max would invent a
  # denominator none of the rows carry.
  one_value <- function(x, what) {
    v <- clean_numeric(x)
    v <- unique(v[!is.na(v)])
    if (length(v) == 0L) return(NA_real_)
    if (length(v) > 1L) {
      stop("CT: ", what, " is not constant within a year/county/grade: ",
           paste(v, collapse = ", "), call. = FALSE)
    }
    v
  }

  # Data label -> all_fips geography_name
  ct_xwalk <- c(
    "Fairfield" = "Fairfield County", "Hartford" = "Hartford County",
    "Litchfield" = "Litchfield County", "Middlesex" = "Middlesex County",
    "New Haven" = "New Haven County", "New London" = "New London County",
    "Tolland" = "Tolland County", "Windham" = "Windham County",
    "Capitol" = "Capitol", "Greater Bridgeport" = "Greater Bridgeport",
    "Lower CT River Valley" = "Lower Connecticut River Valley",
    "Naugatuck Valley" = "Naugatuck Valley",
    "Northeast CT" = "Northeastern Connecticut",
    "Northwest Hills" = "Northwest Hills",
    "South Central" = "South Central Connecticut",
    "Southeastern CT" = "Southeastern Connecticut",
    "Western" = "Western Connecticut"
  )

  # Every series the source publishes, on the repo's canonical antigen keys.
  # "All" is CT's composite across the whole required schedule, so it becomes
  # all_required, matching rate_all_required in the measure dictionary;
  # "MCV" is the 7th-grade meningococcal conjugate requirement (MenACWY).
  vax_map <- c(
    "All" = "all_required", "DTaP" = "dtap", "Tdap" = "tdap",
    "Polio" = "polio", "MMR" = "mmr", "HepA" = "hep_a", "HepB" = "hep_b",
    "Varicella" = "varicella", "MCV" = "menacwy", "Flu" = "flu"
  )
  unmapped <- setdiff(unique(raw$vaccine_series), names(vax_map))
  if (length(unmapped)) {
    stop("CT: unmapped vaccine_series: ", paste(unmapped, collapse = ", "),
         call. = FALSE)
  }

  # Source column -> the suffix it takes on the per-vaccine output column.
  # percentage_vaccinated is the coverage rate and so carries no suffix.
  # total_vaccinated_count is deliberately NOT carried: the source leaves it
  # empty for every year before 2019-20, so as a column it would be a
  # half-populated numerator for a rate that is already published.
  measure_suffix <- c(
    percentage_vaccinated = "",
    percentage_religious_exemption = "_religious_exempt",
    percentage_medical_exemption = "_medical_exempt",
    percentage_non_compliant = "_non_compliant"
  )

  socrata_long <- raw %>%
    mutate(vkey = unname(vax_map[vaccine_series])) %>%
    select(school_year, county_equivalent, grade, vkey,
           all_of(names(measure_suffix))) %>%
    pivot_longer(all_of(names(measure_suffix)),
                 names_to = "measure", values_to = "value") %>%
    mutate(
      value = clean_numeric(value),
      column = paste0("pct_", vkey, unname(measure_suffix[measure]))
    )

  # A "percentage" far outside [0, 100] is not a measurement, so it is dropped
  # to NA rather than carried through as a rate.
  #
  # The small negatives are kept: percentage_non_compliant runs to -1.6 in six
  # rows because the source computes 100 - vaccinated - exemptions from parts it
  # has already rounded to one decimal, and that is a real published figure.
  # 2025-26 Northwest Hills 7th-grade HepA is a different thing -- it prints
  # 993.0 compliant against 95.4 vaccinated and 4.0 exempt, so the decimal point
  # is in the wrong place and the -893.0 beside it is not a rate of anything.
  PCT_BOUNDS <- c(-5, 105)
  impossible <- with(socrata_long, !is.na(value) &
                       (value < PCT_BOUNDS[1] | value > PCT_BOUNDS[2]))
  if (any(impossible)) {
    bad <- socrata_long[impossible, ]
    message(sprintf(
      "Connecticut: dropping %d source percentage(s) outside [%g, %g]:\n  %s",
      nrow(bad), PCT_BOUNDS[1], PCT_BOUNDS[2],
      paste(sprintf("%s %s %s %s = %g", bad$school_year, bad$county_equivalent,
                    bad$grade, bad$column, bad$value), collapse = "\n  ")))
    socrata_long$value[impossible] <- NA_real_
  }

  socrata_wide <- socrata_long %>%
    pivot_wider(id_cols = c(school_year, county_equivalent, grade),
                names_from = column, values_from = value)

  enrolled <- raw %>%
    group_by(school_year, county_equivalent, grade) %>%
    summarise(
      N_enrolled = one_value(total_enrollment_count, "total_enrollment_count"),
      .groups = "drop"
    )

  # A county cannot enrol as many pupils as the whole state, so a county row
  # holding the statewide figure is the statewide figure misfiled, not that
  # county's enrolment. 2012-13 Windham Pre-K is the case in hand: it carries
  # the state's 20,397 pupils and 452 schools against a true figure of roughly
  # 770, and left in place it both breaks the county/state accounting and would
  # dominate any enrolment-weighted roll-up of CT's Pre-K rates.
  #
  # That row's two exemption percentages (1.4 religious, 1.0 medical) are the
  # state's as well, but its coverage and non-compliance are its own (88.0 and
  # 9.5 against the state's 80.3 and 14.5), so only the enrolment -- the one
  # field the error can be proved on -- is dropped. Read Windham's 2012-13
  # Pre-K exemption rates as the statewide figures.
  statewide_enrolled <- enrolled %>%
    filter(county_equivalent == "State") %>%
    select(school_year, grade, N_state = N_enrolled)
  enrolled <- enrolled %>%
    left_join(statewide_enrolled, by = c("school_year", "grade")) %>%
    mutate(bad = county_equivalent != "State" & !is.na(N_enrolled) &
             !is.na(N_state) & N_enrolled >= N_state)
  if (any(enrolled$bad)) {
    message(sprintf(
      paste0("Connecticut: dropping %d county enrolment(s) that equal or ",
             "exceed the statewide row:\n  %s"),
      sum(enrolled$bad),
      paste(sprintf("%s %s %s: N_enrolled=%g vs state %g",
                    enrolled$school_year[enrolled$bad],
                    enrolled$county_equivalent[enrolled$bad],
                    enrolled$grade[enrolled$bad],
                    enrolled$N_enrolled[enrolled$bad],
                    enrolled$N_state[enrolled$bad]), collapse = "\n  ")))
    enrolled$N_enrolled[enrolled$bad] <- NA_real_
  }
  enrolled <- enrolled %>%
    select(school_year, county_equivalent, grade, N_enrolled)

  socrata <- socrata_wide %>%
    left_join(enrolled, by = c("school_year", "county_equivalent", "grade")) %>%
    mutate(end_year = school_year_end_from_label(school_year))

  # ---- All-grades exemption workbook ----
  # The workbook publishes PROPORTIONS (0.0210) while the Socrata extract
  # publishes PERCENT POINTS (2.1), and both land in the same
  # pct_all_required_*_exempt columns. Two scales in one column is the exact
  # failure resources/rate_scale.R exists to prevent, so the workbook is put on
  # percent points here and the whole frame is declared `from = "percent"` on
  # write.
  all_grades <- read_excel(all_grades_file, .name_repair = "minimal") %>%
    transmute(
      school_year = as.character(`School Year`),
      county_equivalent = as.character(County),
      grade = "All Grades",
      # No vaccine breakdown and no grade breakdown: these are the county's
      # exemption rates across the whole required schedule and every grade, so
      # they share the all_required columns with CT's own "All" series.
      pct_all_required_religious_exempt =
        clean_numeric(Exemption_Religious) * 100,
      pct_all_required_medical_exempt = clean_numeric(Exemption_Medical) * 100,
      pct_all_required_full_exempt = clean_numeric(Exemption_Total) * 100,
      end_year = school_year_end_from_label(`School Year`)
    )
  # N_enrolled and the coverage rates are NA for these rows: the workbook
  # publishes exemption shares only, with no denominator and no coverage.

  combined <- bind_rows(socrata, all_grades) %>%
    mutate(
      time = as.Date(school_year_time_from_end(end_year)),
      # ct_xwalk maps DPH's abbreviated planning-region labels onto their
      # census names. An unmapped label falls through as itself so that
      # join_county_fips() can name it in the error instead of quietly
      # booking it as a statewide row.
      fips_name = if_else(
        county_equivalent == "State", "Connecticut",
        coalesce(unname(ct_xwalk[county_equivalent]), county_equivalent)
      )
    ) %>%
    join_county_fips("CT", county_col = "fips_name", statewide = "Connecticut")

  # Coverage, then the two exemption categories, then non-compliance, grouped
  # by vaccine -- pivot_wider() otherwise orders the columns by whichever row
  # happened to come first.
  pct_order <- as.vector(t(outer(
    paste0("pct_", unname(vax_map)),
    c("", "_religious_exempt", "_medical_exempt", "_full_exempt",
      "_non_compliant"),
    paste0
  )))
  pct_cols <- grep("^pct_", names(combined), value = TRUE)
  unordered <- setdiff(pct_cols, pct_order)
  if (length(unordered)) {
    stop("CT: pct_order does not name: ", paste(unordered, collapse = ", "),
         call. = FALSE)
  }

  data_out <- combined %>%
    select(time, geography, geography_name, grade, N_enrolled,
           all_of(intersect(pct_order, pct_cols))) %>%
    filter(!is.na(time)) %>%
    arrange(time, geography_name, grade)

  dir.create("standard", showWarnings = FALSE)
  # write_standard() returns the frame it wrote -- pct_* already converted to
  # rate_* on the 0-1 scale, count columns canonicalised, and all-NA measures
  # dropped -- so the four per-grade files below are filtered from those same
  # values rather than being built a second time from `combined`.
  wide <- write_standard(data_out, "Connecticut", "./standard/data.csv.gz",
                         from = "percent")

  # ---- Per-grade files --------------------------------------------------------
  # data.csv.gz stacks every grade, so a Pre-K row carries 32 empty antigen
  # columns it will never have a value for, and a 7th/K row carries the four
  # empty rate_flu* columns Pre-K owns exclusively. Filtering to one grade and
  # re-running write_standard()'s drop_empty_measures() on just that subset
  # removes the columns that are structurally empty for THAT grade -- same
  # values, same wide shape, less missing data per file. `from = "rate"`
  # because `wide` is already on the 0-1 scale; there is no pct_ column left to
  # convert.
  #
  # These are convenience files, not the pipeline's input: both
  # scripts/build_all_states_county_standard.R and
  # scripts/generate_measure_info.R read data.csv.gz by that exact name, so it
  # stays the all-grades file and keeps feeding both.
  grade_files <- c(
    "7th"        = "data_7th.csv.gz",
    "K"          = "data_k.csv.gz",
    "Pre-K"      = "data_pre_k.csv.gz",
    "All Grades" = "data_all_grades.csv.gz"
  )
  for (g in names(grade_files)) {
    path <- file.path("standard", grade_files[[g]])
    write_standard(wide %>% filter(grade == g),
                   sprintf("Connecticut (%s)", g), path, from = "rate")
  }

  update_latest_year(latest_school_year(wide %>% filter(grade != "All Grades")),
                     ids = "socrata_county")
  update_latest_year(latest_school_year(wide %>% filter(grade == "All Grades")),
                     ids = "all_grades_workbook")

  # ---- By-school tables -------------------------------------------------------
  # One row per school x grade x survey year, in standard/data_schools.csv.gz.
  # The kindergarten and seventh-grade tables give per-antigen coverage and
  # the three exemption shares; the all-grades table gives the exemption
  # shares only. Every figure is a percent of the school's students in that
  # grade (DPH: "the number of students with the required number of doses
  # divided by the total number of students"), on percent points like the
  # county extract, and there is no enrolment column, so no denominator is
  # carried.
  #
  # Two markers stand in for a figure, always for every measure on the row:
  #   DS   Data Suppressed -- a small school, withheld to protect the count.
  #        The value is NA and flag_<measure> is "suppressed".
  #   DNC  Did Not Complete -- the school did not finish the survey, so there
  #        is nothing behind the cell. The value is NA and the flag is
  #        "missing". Neither marker is one of the forms is_censored()
  #        recognises, so the flags are set here rather than by censor_flag().
  # The county extract has no markers at all, which is why this is the first
  # place in the CT ingest that carries a flag column.
  #
  # Column keys match the county file's vax_map, so rate_hep_b here is the
  # same measure as rate_hep_b there, and "All" becomes all_required. The
  # exemption shares are not broken out by antigen in these tables -- they are
  # the share of students exempt from anything -- so they take the
  # all_required prefix, as the county file's own "All" series and the
  # all-grades workbook do.
  school_vax_map <- c(
    polio = "polio", dtap = "dtap", mmr = "mmr", hepb = "hep_b",
    varicella = "varicella", var = "varicella", hepa = "hep_a",
    mcv = "menacwy", tdap = "tdap", all = "all_required"
  )
  school_exempt_map <- c(
    ex_rel = "all_required_religious_exempt",
    ex_med = "all_required_medical_exempt",
    ex_tot = "all_required_full_exempt"
  )
  school_grade <- c(k = "K", `7` = "7th", exempt = "All Grades")

  school_flag <- function(x) {
    chr <- trimws(as.character(x))
    out <- rep(CENSOR_FLAG_NONE, length(chr))
    out[is.na(chr) | chr == ""] <- NA_character_
    out[!is.na(chr) & toupper(chr) == "DS"] <- "suppressed"
    out[!is.na(chr) & toupper(chr) == "DNC"] <- "missing"
    out
  }

  # The by-school tables spell one planning region two ways ("Western" and
  # "Western CT") within the same year; both are the Western Connecticut
  # region. "Middlesex" also appears once, on a row whose city field holds
  # the school's name instead of a town; ct_xwalk resolves it to Middlesex
  # County (09007), which is what DPH wrote, and it is carried as such rather
  # than guessed onto a planning region.
  school_xwalk <- c(ct_xwalk, "Western CT" = "Western Connecticut")

  read_school_file <- function(path) {
    m <- str_match(basename(path), "^ct_school_(k|7|exempt)_(\\d{4})\\.csv$")
    kind <- m[, 2]
    start_year <- as.integer(m[, 3])
    d <- readr::read_csv(path, show_col_types = FALSE,
                         col_types = readr::cols(.default = "c"))
    measure_map <- c(school_vax_map, school_exempt_map)
    present <- intersect(names(d), names(measure_map))
    unknown <- setdiff(names(d), c("school_name", "school_type", "address", "city",
                                   "zipcode", "planning_region", present))
    if (length(unknown)) {
      stop("CT: unmapped column(s) in ", basename(path), ": ",
           paste(unknown, collapse = ", "), call. = FALSE)
    }
    # DS and DNC are row-level statuses; a row with a marker in one measure
    # and a number in another would be a layout change worth knowing about.
    marker <- vapply(present, function(cc) toupper(trimws(d[[cc]])) %in% c("DS", "DNC"),
                     logical(nrow(d)))
    if (is.matrix(marker) && any(rowSums(marker) %% length(present) != 0)) {
      stop("CT: DS/DNC marks part of a row in ", basename(path), call. = FALSE)
    }
    out <- tibble(
      time = as.Date(school_year_time(start_year)),
      grade = unname(school_grade[kind]),
      school_name = d$school_name,
      school_type = d$school_type,
      city = d$city,
      planning_region = d$planning_region
    )
    for (cc in present) {
      key <- measure_map[[cc]]
      out[[paste0("pct_", key)]] <- clean_numeric(d[[cc]])
      out[[paste0("flag_", key)]] <- school_flag(d[[cc]])
    }
    out
  }

  school_files <- list.files("raw", "^ct_school_(k|7|exempt)_\\d{4}\\.csv$",
                             full.names = TRUE)
  if (length(school_files)) {
    schools <- bind_rows(lapply(school_files, read_school_file)) %>%
      mutate(fips_name = coalesce(unname(school_xwalk[planning_region]),
                                  planning_region)) %>%
      join_county_fips("CT", county_col = "fips_name") %>%
      select(-fips_name)

    school_pct <- grep("^pct_", names(schools), value = TRUE)
    school_order <- as.vector(t(outer(
      paste0("pct_", unname(school_vax_map)),
      c("", "_religious_exempt", "_medical_exempt", "_full_exempt"),
      paste0
    )))
    school_order <- unique(school_order)
    unordered <- setdiff(school_pct, school_order)
    if (length(unordered)) {
      stop("CT: school_order does not name: ", paste(unordered, collapse = ", "),
           call. = FALSE)
    }
    ordered <- intersect(school_order, school_pct)
    schools_out <- schools %>%
      select(time, geography, geography_name, grade, school_name, school_type,
             city, planning_region,
             all_of(as.vector(rbind(ordered, sub("^pct_", "flag_", ordered))))) %>%
      arrange(time, grade, geography_name, school_name)

    message(sprintf(
      "Connecticut schools: %d rows, %s, %s",
      nrow(schools_out),
      paste(sprintf("%s=%d", names(table(schools_out$grade)), table(schools_out$grade)),
            collapse = " "),
      paste(range(schools_out$time), collapse = " to ")))

    schools_wide <- write_standard(schools_out, "Connecticut (schools)",
                                   "./standard/data_schools.csv.gz",
                                   from = "percent")
    for (kind in names(school_grade)) {
      update_latest_year(
        latest_school_year(schools_wide %>% filter(grade == school_grade[[kind]])),
        ids = c(k = "socrata_school_k", `7` = "socrata_school_7",
                exempt = "socrata_school_exempt_all")[[kind]]
      )
    }
  }

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
