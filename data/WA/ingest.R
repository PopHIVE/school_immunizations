library(dcf)
library(dplyr)
library(tidyr)
library(stringr)
library(readxl)
library(readr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")

# =============================================================================
# WA - School immunization status by grade and vaccine: state, county and
#      school district
# Source: Washington State Department of Health, Washington Tracking Network
#   school immunization dashboard. DOH posts one workbook per school year,
#   <YYYY>-<YYYY>SchoolYear.xlsx, 2016-17 onward, with three sheets (State,
#   County, School District) in one long layout:
#     School Year | Geography | Grade | Disease or Vaccine |
#     Immunization Status | Count | Enrollment | Percent | County/School District
#   Percent is a proportion (0-1) and is Count / Enrollment to machine
#   precision in every row, so rates here are computed from the two counts
#   and the published share is only used to police them.
#
#   The layout changed once. 2016-17 through 2018-19 report per-vaccine rows
#   for DT, Pertussis, Polio, MMR, Hepatitis B and Varicella with a single
#   status each: "Exempt" for K-12 at every level and for every grade on the
#   district sheet, but "Incomplete" for Kindergarten and Sixth Grade on the
#   State and County sheets (so county per-vaccine exemptions exist only for
#   K-12 in those years). From 2019-20 the per-antigen rows are Diphtheria,
#   Tetanus, Pertussis, Measles, Mumps, Rubella, Hepatitis B, Varicella and
#   Polio, each with the full status set, and Complete + Conditional + Out of
#   Compliance + Exempt equals Enrollment in every per-antigen row. The
#   middle-school cohort is "Sixth Grade" through 2019-20 and "Seventh Grade"
#   from 2020-21. Measles, Mumps and Rubella carry no "Personal Exemption"
#   row from 2019-20 on (the personal exemption for MMR was removed by law in
#   July 2019); "Overall" still does.
#
#   Emitted: for "Overall", every status (complete, conditional, out of
#   compliance, exempt and the four exemption reasons) as N_ and rate_; for
#   each antigen, the "Complete" count/rate as N_<vax>/rate_<vax>, the
#   "Exempt" count/rate as N_<vax>_exempt/rate_<vax>_exempt, and the 2016-19
#   "Incomplete" count/rate as N_<vax>_incomplete/rate_<vax>_incomplete. The
#   source does not define "Incomplete"; it is always at most the overall
#   Conditional + Out of Compliance + Exempt for the same row, and the later
#   layout's exact partition suggests it is Enrollment minus Complete for
#   that antigen, but it is carried as published rather than turned into a
#   coverage figure. Per-antigen conditional, out-of-compliance and
#   exemption-reason rows are in the source but not carried.
#
#   Source quirks handled below:
#   * 2025-26 renames the district sheet "School_District", puts a title row
#     ("DOH 348-1176 June 2026") above the header on the State sheet, and
#     appends an accessibility footer under the State table. The header row is
#     located by content and rows with no Geography are dropped.
#   * 2016-17 and 2017-18 list a few districts (Ferndale, Highline, Kittitas,
#     Seattle, Spokane, Tacoma) two or three times per grade with different
#     counts and enrollments -- separate reporting units under one district
#     name (Tacoma K-12 is 32,044 + 139 + 132). They are summed, counts and
#     enrollments both, so the district rate is a pooled rate.
#   * K-12 "Conditional" and "Out of Compliance" are not reported for 2016-17
#     through 2018-19: the state row omits them, and every county and district
#     row prints a count of 0 (with no Percent at district level) while
#     Complete + Exempt falls short of Enrollment by thousands. Those two
#     measures are set to NA for K-12 in those years rather than published as
#     zero.
#   * One negative count: Asotin-Anatone School District, 2023-24, K-12,
#     Varicella, Conditional = -2. Negative counts become NA (that row is not
#     an emitted measure anyway).
#   * Wahkiakum County is absent from the 2019-20 workbook and has no Seventh
#     Grade rows in 2025-26; Skamania has no Kindergarten rows in 2018-19.
#   * "Exempt" is students with any exemption and is not the sum of the four
#     reason rows (a student can hold more than one). Both are carried as
#     published.
#
#   DOH also posts a per-school workbook for each year (SchoolBuilding.xlsx,
#   about 23 MB each; 2025-26 is named 3481175-2025-2026BuildingRates.xlsx).
#   Not downloaded here; noted in sources.json.
#
#   raw/Washington Vaccine Exemption.xlsx is the county exemption workbook
#   obtained from DOH by request (Kindergarten and K-12 2018-19 to 2023-24,
#   7th grade 2020-21 to 2023-24) that this ingest used to be built on. It
#   was reconciled against the public workbooks: all 7th-grade rows and all
#   but five counties' Kindergarten and K-12 rows agree exactly on every
#   count; the five (Asotin, Snohomish, Spokane, Whatcom, Yakima, 2021-22
#   only) differ in enrollment and counts in a way that looks like an
#   earlier snapshot of that year (Asotin Kindergarten enrollment 91 against
#   202). Its "Religious %" column on the 7th-grade sheet is not Religious
#   Count / Enrollment in 133 of 155 rows (it repeats the Personal % column);
#   the public workbook's religious counts match it exactly and its shares
#   are consistent, which settles the discrepancy the old ingest documented.
#   The request workbook alone has four county-year rows the public files
#   omit (Skamania 2018-19 Kindergarten; Wahkiakum 2019-20 Kindergarten and
#   K-12; Wahkiakum 2020-21 7th grade). It is no longer parsed; the public
#   workbooks are the sole source.
#
# Second source: data.wa.gov school-level tables, 2014-15 to 2016-17
#   Before the WTN workbooks, DOH published the per-school survey results as
#   Socrata datasets on data.wa.gov, one per school year and cohort
#   (Kindergarten, Sixth Grade, K-12), listed in sources.json under
#   socrata_school_datasets with their ids. The series is closed: nothing
#   was added after 2016-17, and later years are the workbooks above. Each
#   file is fetched once and kept (if_exists = "skip"). They are parsed into
#   standard/data_schools_2014_2016.csv.gz, one row per school and cohort,
#   and are not aggregated to county.
#
#   Every file lists every school in the state (about 2,600 rows) with
#   school, district, county, ESD, grade span, address, a Reported Y/N flag
#   and, for the cohort, an enrollment plus percent and count columns:
#     Kindergarten and Sixth Grade: reported_enrollment; complete,
#       conditional, out of compliance, any exemption and the four exemption
#       reasons; per antigen (diphtheria_tetanus, pertussis,
#       measles_mumps_rubella, polio, hepatitisb, varicella) a percent
#       complete and a number incomplete.
#     K-12: k_12_enrollment; complete, any exemption and the four reasons;
#       per antigen a percent and number exempt. No conditional or out of
#       compliance, as in the K-12 rows of the 2016-19 workbooks.
#   Percents are percent points to one decimal and equal count / enrollment
#   on every row, and the per-antigen percent complete equals
#   1 - incomplete / enrollment exactly, so N_<vax> is enrollment minus the
#   incomplete count and every rate is computed from counts, as above. The
#   measure names are the workbook block's: the antigens are dt, pertussis,
#   mmr, polio, hep_b and varicella, the 2016-19 workbook set.
#
#   Rows kept: schools with enrollment > 0 in the cohort (a school without
#   the grade carries enrollment 0 and zeros throughout; the has_kindergarten
#   and has_6thgrade flags disagree with the enrollment on a few dozen rows
#   a year, so the enrollment decides), plus schools with Reported = N whose
#   flag says they have the grade (all of them for K-12). A non-reporting
#   school carries no values and flag_complete = "missing".
#
#   Source quirks:
#   * The dataset titled "Sixth (6th) grade immunization data, 2014-2015"
#     (mgne-w2kv) is the K-12 2014-15 table (i89p-imif) row for row, with
#     the K-12 columns; there is no sixth-grade table for 2014-15. It is not
#     fetched.
#   * Two cells carry a count larger than the enrollment (Easton School
#     2014-15 religious membership exemptions, 324 of 108; Cornerstone
#     Christian School 2015-16 varicella exemptions, 718 of 106). Those
#     counts and rates are set to NA.
#   * School type is only in the 2015-16 and 2016-17 Kindergarten and Sixth
#     Grade files; address and city are absent from the 2016-17 files, so
#     neither is carried.
#   * The 2016-17 totals agree with the workbook's statewide row: Sixth
#     Grade exactly, Kindergarten and K-12 within 0.1 percent.
# =============================================================================

sources <- read_sources()
src <- source_entry(sources, "doh_school_year_workbooks")
process <- dcf::dcf_process_record()
prev <- process$fetch_state

# Per-year files that do not change once posted, so files already on disk are
# skipped. raw/ is committed, so a failure to reach the index page warns and
# the ingest proceeds on what is here. The 2025-26 link carries a document
# number prefix (3481176-2025-2026SchoolYear.xlsx); the dest drops it so each
# year has one stable path.
links <- discover_links(src$page_url, src$pattern, must_find = FALSE)
if (isTRUE(attr(links, "fetched")) && !nrow(links)) {
  warning("WA: index page loaded but no link matched /", src$pattern, "/; ",
          length(attr(links, "candidates")), " other data links on the page",
          call. = FALSE)
}
sy_dest <- function(u) {
  file.path("raw", str_extract(basename(u), "\\d{4}-\\d{4}SchoolYear\\.xlsx$"))
}
sy_validate <- function(path) {
  sh <- readxl::excel_sheets(path)
  if (!all(c("State", "County") %in% sh)) {
    stop("expected State and County sheets, found: ", paste(sh, collapse = ", "))
  }
}
recs <- fetch_many(links$url, dest_fn = sy_dest, if_exists = "skip", type = "xlsx",
                   validate = sy_validate, previous = prev)
process <- record_fetch(process, recs)

# The closed data.wa.gov series: one CSV per dataset id, kept once fetched.
# A file already in raw/ that validates is not requested again; a dataset
# that cannot be reached and has no committed copy stops the ingest, since
# nothing is on disk to parse for it.
sch_src <- source_entry(sources, "socrata_school_datasets")
sch_recs <- list()
for (ds in sch_src$datasets) {
  sch_recs[[ds$dest]] <- socrata_csv(
    "data.wa.gov", ds$id, ds$dest,
    expect_cols = c("school_name", "school_year", "reported", "school_district", "county"),
    if_exists = "skip", previous = prev[[ds$dest]]
  )
  if (sch_recs[[ds$dest]]$status %in% c("updated", "failed")) Sys.sleep(2)
}
process <- record_fetch(process, sch_recs)

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

# Source labels -> measure names. Anything not listed stops the build, so a
# renamed or added category surfaces instead of being silently dropped.
OVERALL_STATUS <- c(
  "Complete" = "complete",
  "Conditional" = "conditional",
  "Out of Compliance" = "out_of_compliance",
  "Exempt" = "full_exempt",
  "Personal Exemption" = "personal_exempt",
  "Medical Exemption" = "medical_exempt",
  "Religious Exemption" = "religious_exempt",
  "Religious Membership Exemption" = "religious_membership_exempt"
)
KNOWN_STATUS <- c(names(OVERALL_STATUS), "Incomplete")
VACCINES <- c(
  "DT" = "dt", "Pertussis" = "pertussis", "Polio" = "polio", "MMR" = "mmr",
  "Hepatitis B" = "hep_b", "Varicella" = "varicella",
  "Diphtheria" = "diphtheria", "Tetanus" = "tetanus", "Measles" = "measles",
  "Mumps" = "mumps", "Rubella" = "rubella"
)
GRADES <- c(
  "Kindergarten" = "Kindergarten", "Sixth Grade" = "6th grade",
  "Seventh Grade" = "7th grade", "K-12" = "K-12"
)
MEASURE_ORDER <- c(unname(OVERALL_STATUS), unname(VACCINES),
                   paste0(unname(VACCINES), "_exempt"),
                   paste0(unname(VACCINES), "_incomplete"))

# Header row found by content: the 2025-26 State sheet has a title row above
# it, the others start on row 1.
read_wa_sheet <- function(path, sheet) {
  raw <- readxl::read_excel(path, sheet = sheet, col_names = FALSE,
                            col_types = "text", .name_repair = "minimal")
  hdr <- which(!is.na(raw[[1]]) & raw[[1]] == "School Year")[1]
  if (is.na(hdr)) {
    stop("WA: no 'School Year' header row in ", basename(path), " sheet ", sheet)
  }
  names(raw) <- as.character(unlist(raw[hdr, ]))
  d <- raw[-seq_len(hdr), , drop = FALSE]
  need <- c("School Year", "Geography", "Grade", "Disease or Vaccine",
            "Immunization Status", "Count", "Enrollment", "Percent")
  if (!all(need %in% names(d))) {
    stop("WA: ", basename(path), " sheet ", sheet, " lacks column(s): ",
         paste(setdiff(need, names(d)), collapse = ", "))
  }
  d[!is.na(d$Geography), , drop = FALSE]
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  files <- list.files("raw", pattern = "^\\d{4}-\\d{4}SchoolYear\\.xlsx$", full.names = TRUE)
  if (!length(files)) stop("WA: no raw/<YYYY>-<YYYY>SchoolYear.xlsx workbooks to parse")

  long <- bind_rows(lapply(files, function(f) {
    sheets <- readxl::excel_sheets(f)
    keep <- sheets[sheets %in% c("State", "County") | grepl("^School.?District$", sheets)]
    if (length(keep) != 3) {
      stop("WA: expected State, County and School District sheets in ", basename(f),
           ", found: ", paste(sheets, collapse = ", "))
    }
    bind_rows(lapply(keep, function(s) {
      d <- read_wa_sheet(f, s)
      unit <- if ("County" %in% names(d)) {
        d$County
      } else if ("School District" %in% names(d)) {
        d$`School District`
      } else {
        rep(NA_character_, nrow(d))
      }
      tibble(
        file = basename(f),
        school_year = str_trim(d$`School Year`),
        geo = str_trim(d$Geography),
        unit = str_trim(unit),
        grade_src = str_trim(d$Grade),
        disease = str_trim(d$`Disease or Vaccine`),
        status = str_trim(d$`Immunization Status`),
        count = clean_numeric(d$Count),
        enrollment = clean_numeric(d$Enrollment),
        percent = clean_numeric(d$Percent)
      )
    }))
  }))

  # Every label must be one this script knows.
  stop_on_unknown <- function(values, known, what) {
    bad <- setdiff(unique(values), known)
    if (length(bad)) {
      stop("WA: unrecognised ", what, ": ", paste(bad, collapse = ", "), call. = FALSE)
    }
  }
  stop_on_unknown(long$geo, c("State", "County", "District"), "Geography")
  stop_on_unknown(long$grade_src, names(GRADES), "Grade")
  stop_on_unknown(long$disease, c("Overall", names(VACCINES)), "Disease or Vaccine")
  stop_on_unknown(long$status, KNOWN_STATUS, "Immunization Status")
  if (any(is.na(long$unit[long$geo != "State"]))) {
    stop("WA: county or district rows with no County / School District label")
  }
  bad_year <- long$school_year[is.na(school_year_end_from_label(long$school_year))]
  if (length(bad_year)) {
    stop("WA: School Year labels that do not parse: ", paste(unique(bad_year), collapse = ", "))
  }

  # The published Percent is a proportion; confirm it against the counts
  # before it is discarded.
  check_rate_against_counts(long$percent, long$count, long$enrollment,
                            label = "WA Percent", tol = 0.001)

  neg <- !is.na(long$count) & long$count < 0
  if (any(neg)) {
    message(sprintf("WA: %d negative count(s) set to NA: %s", sum(neg),
                    paste(unique(sprintf("%s %s %s %s %s", long$unit[neg], long$school_year[neg],
                                         long$grade_src[neg], long$disease[neg], long$status[neg])),
                          collapse = "; ")))
    long$count[neg] <- NA_real_
  }

  # Pool the multi-unit districts (2016-17, 2017-18); a no-op elsewhere.
  long <- long %>%
    group_by(school_year, geo, unit, grade_src, disease, status) %>%
    summarise(count = sum(count), enrollment = sum(enrollment), .groups = "drop")

  long <- long %>%
    mutate(
      measure = case_when(
        disease == "Overall" ~ unname(OVERALL_STATUS[status]),
        status == "Complete" ~ unname(VACCINES[disease]),
        status == "Exempt" ~ paste0(unname(VACCINES[disease]), "_exempt"),
        status == "Incomplete" ~ paste0(unname(VACCINES[disease]), "_incomplete"),
        TRUE ~ NA_character_
      ),
      rate = rate_from_counts(count, enrollment)
    ) %>%
    filter(!is.na(measure))

  # One enrollment per (year, geography, grade): it is identical across every
  # disease and status row of a unit in every published year.
  enrolled <- long %>%
    filter(disease == "Overall", status == "Complete") %>%
    select(school_year, geo, unit, grade_src, N_enrolled = enrollment)

  wide <- long %>%
    select(school_year, geo, unit, grade_src, measure, count, rate) %>%
    pivot_wider(names_from = measure, values_from = c(count, rate),
                names_glue = "{.value}_{measure}") %>%
    rename_with(~ sub("^count_", "N_", .x), starts_with("count_")) %>%
    left_join(enrolled, by = c("school_year", "geo", "unit", "grade_src")) %>%
    mutate(
      time = school_year_time_from_end(school_year_end_from_label(school_year)),
      grade = unname(GRADES[grade_src])
    )

  # K-12 Conditional / Out of Compliance not reported before 2019-20 (see
  # header); the zeros in the county and district rows are placeholders.
  not_reported <- wide$grade == "K-12" & wide$time < "2019-09-01"
  for (col in c("N_conditional", "rate_conditional",
                "N_out_of_compliance", "rate_out_of_compliance")) {
    wide[[col]][not_reported] <- NA_real_
  }

  measure_cols <- unlist(lapply(MEASURE_ORDER, function(m) paste0(c("N_", "rate_"), m)))
  measure_cols <- intersect(measure_cols, names(wide))

  state <- wide %>%
    filter(geo == "State") %>%
    mutate(geography = "53", geography_name = "Washington", type = "state")

  counties <- wide %>%
    filter(geo == "County") %>%
    rename(county = unit) %>%
    join_county_fips("WA") %>%
    mutate(type = "county")

  data <- bind_rows(counties, state) %>%
    select(time, geography, geography_name, type, grade, N_enrolled, all_of(measure_cols)) %>%
    arrange(time, type, geography, grade)

  districts <- wide %>%
    filter(geo == "District") %>%
    transmute(time, geography = NA_character_, geography_name = unit, district = unit,
              grade, N_enrolled, !!!rlang::syms(measure_cols)) %>%
    arrange(time, district, grade)

  message(sprintf(
    "WA: %d county/state rows (%d county, %d state), %d district rows; grades: %s; time %s to %s",
    nrow(data), sum(data$type == "county"), sum(data$type == "state"), nrow(districts),
    paste(sort(unique(data$grade)), collapse = ", "), min(data$time), max(data$time)))

  dir.create("standard", showWarnings = FALSE)
  # Rates are computed from Count / Enrollment above, so there are no
  # percent-scaled columns to convert; "rate" declares the scale they are on.
  out <- write_standard(data, "Washington", "standard/data.csv.gz", from = "rate")
  write_standard(districts, "Washington districts", "standard/data_districts.csv.gz", from = "rate")
  update_latest_year(latest_school_year(out), ids = "doh_school_year_workbooks")

  # ---- data.wa.gov school-level tables --------------------------------------
  SCH_STATUS <- c(
    "complete_for_all_immunizations" = "complete",
    "conditional" = "conditional",
    "out_of_compliance" = "out_of_compliance",
    "with_any_exemption" = "full_exempt",
    "with_personal_exemption" = "personal_exempt",
    "with_medical_exemption" = "medical_exempt",
    "with_religious_exemption" = "religious_exempt",
    "with_religious_membership_exemption" = "religious_membership_exempt"
  )
  SCH_ANTIGENS <- c(
    "diphtheria_tetanus" = "dt", "pertussis" = "pertussis",
    "measles_mumps_rubella" = "mmr", "polio" = "polio",
    "hepatitisb" = "hep_b", "varicella" = "varicella"
  )
  # Column stem (after number_ / percent_) -> measure name; anything else
  # stops the build.
  sch_measure <- function(stem, file) {
    if (stem %in% names(SCH_STATUS)) return(unname(SCH_STATUS[stem]))
    m <- str_match(stem, "^(complete|incomplete|exempt)_for_(.+)$")
    if (is.na(m[1, 1]) || !m[1, 3] %in% names(SCH_ANTIGENS)) {
      stop("WA: unrecognised column in ", file, ": ", stem, call. = FALSE)
    }
    v <- unname(SCH_ANTIGENS[m[1, 3]])
    switch(m[1, 2], complete = v, incomplete = paste0(v, "_incomplete"),
           exempt = paste0(v, "_exempt"))
  }

  read_sch <- function(ds) {
    if (!file.exists(ds$dest)) stop("WA: ", ds$dest, " is missing", call. = FALSE)
    d <- readr::read_csv(ds$dest, show_col_types = FALSE, progress = FALSE,
                         col_types = readr::cols(.default = "c"))
    enr_col <- if (ds$grade == "K-12") "k_12_enrollment" else "reported_enrollment"
    need <- c("school_name", "school_year", "reported", "school_district", "county", enr_col)
    if (!all(need %in% names(d))) {
      stop("WA: ", ds$dest, " lacks column(s): ", paste(setdiff(need, names(d)), collapse = ", "),
           call. = FALSE)
    }
    start <- school_year_end_from_label(d$school_year) - 1L
    if (any(is.na(start)) || any(start != ds$year)) {
      stop("WA: ", ds$dest, " school_year values (", paste(unique(d$school_year), collapse = ", "),
           ") do not match sources.json year ", ds$year, call. = FALSE)
    }
    reported <- toupper(str_trim(d$reported))
    stop_on_unknown(reported, c("Y", "N"), paste("Reported flag in", ds$dest))
    reported <- reported == "Y"
    enrollment <- clean_numeric(d[[enr_col]])
    if (any(reported & is.na(enrollment)) || any(!reported & !is.na(enrollment))) {
      stop("WA: ", ds$dest, ": Reported flag and enrollment disagree", call. = FALSE)
    }
    has_grade <- switch(ds$grade,
      "Kindergarten" = d$has_kindergarten, "6th grade" = d$has_6thgrade, NULL)
    has_grade <- if (is.null(has_grade)) rep(TRUE, nrow(d)) else toupper(str_trim(has_grade)) %in% c("Y", "1")
    keep <- (reported & enrollment > 0) | (!reported & has_grade)

    out <- tibble(
      time = school_year_time(ds$year),
      grade = ds$grade,
      school_name = str_trim(d$school_name),
      district = str_trim(d$school_district),
      county = str_trim(d$county),
      school_type = if ("school_type" %in% names(d)) str_trim(d$school_type) else NA_character_,
      N_enrolled = enrollment,
      flag_complete = ifelse(reported, "", "missing")
    )
    stems <- unique(sub("^(number|percent)_", "", grep("^(number|percent)_", names(d), value = TRUE)))
    for (stem in stems) {
      m <- sch_measure(stem, ds$dest)
      num_col <- paste0("number_", stem)
      pct_col <- paste0("percent_", stem)
      if (num_col %in% names(d)) {
        num <- clean_numeric(d[[num_col]])
      } else {
        # per-antigen complete: only the percent is published; the count is
        # enrollment minus the antigen's incomplete count
        inc_col <- sub("^complete_", "number_incomplete_", stem)
        if (!inc_col %in% names(d)) stop("WA: ", ds$dest, " has ", pct_col, " but no ", inc_col, call. = FALSE)
        num <- enrollment - clean_numeric(d[[inc_col]])
      }
      bad <- keep & !is.na(num) & !is.na(enrollment) & (num > enrollment | num < 0)
      if (any(bad)) {
        message(sprintf("WA: %s: %d count(s) outside 0..enrollment set to NA: %s", basename(ds$dest),
                        sum(bad), paste(sprintf("%s %s %s of %s", d$school_name[bad], m,
                                                num[bad], enrollment[bad]), collapse = "; ")))
        num[bad] <- NA_real_
      }
      if (pct_col %in% names(d)) {
        check_rate_against_counts(clean_numeric(d[[pct_col]])[keep] / 100, num[keep], enrollment[keep],
                                  label = paste("WA", basename(ds$dest), pct_col), tol = 0.001)
      }
      out[[paste0("N_", m)]] <- num
      out[[paste0("rate_", m)]] <- rate_from_counts(num, enrollment)
    }
    out[keep, , drop = FALSE]
  }

  schools <- bind_rows(lapply(sch_src$datasets, read_sch)) %>%
    join_county_fips("WA") %>%
    mutate(type = "school")
  sch_measure_cols <- intersect(
    unlist(lapply(MEASURE_ORDER, function(m) paste0(c("N_", "rate_"), m))), names(schools))
  schools <- schools %>%
    select(time, geography, geography_name, type, school_name, district, school_type, grade,
           N_enrolled, all_of(sch_measure_cols), flag_complete) %>%
    arrange(time, grade, geography, district, school_name)

  per_cohort <- schools %>% count(time, grade)
  message(sprintf(
    "WA schools: %d rows (%d not reporting); %s",
    nrow(schools), sum(schools$flag_complete == "missing"),
    paste(sprintf("%s %s %d", substr(per_cohort$time, 1, 4), per_cohort$grade, per_cohort$n),
          collapse = "; ")))

  sch_out <- write_standard(schools, "Washington schools",
                            "standard/data_schools_2014_2016.csv.gz", from = "rate")
  update_latest_year(latest_school_year(sch_out), ids = "socrata_school_datasets")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
