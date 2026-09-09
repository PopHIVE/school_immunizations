library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/county_fips.R")
source("../../resources/school_year.R")
source("../../resources/fetch.R")
source("../../resources/pdf_table.R")

# =============================================================================
# AL - School Entry Survey: certificate status and exemptions by county
# Source: Alabama Department of Public Health, Immunization Division.
#
# Two series, both from the annual School Entry Survey:
#
#   * County summary PDFs posted on the survey page
#     (https://www.alabamapublichealth.gov/immunization/school-entry-survey.html),
#     one per school year from 2014-15 to 2020-21, all grades, plus a
#     kindergarten-only table for 2020-21. Each is a single column-aligned
#     table: one row per county grouped under a public-health district (with
#     a district TOTAL row) and a STATE TOTAL. The columns are counts with a
#     percent of enrolment beside each: students enrolled, students holding a
#     current (not expired) Certificate of Immunization, Certificate of
#     Medical Exemption, medical exemption together with a certificate
#     (a partial exemption, up to date otherwise), the same two for religious
#     exemptions, expired certificate, and no certificate on file. The column
#     order and count change between years (see PDF_LAYOUTS), so every file
#     carries a declared layout and a file without one stops the run.
#
#     Two files need a note. 2019-2020schoolsurvey_county.pdf covers public
#     schools only (its title says so; the other years are public and
#     private), and it is the only survey year with two county tables.
#     2020schoolsurvey_county.pdf is titled "2019-2020" but is the 2020-21
#     survey: the page lists it as "2020 by County" (the page names each
#     file by the school-year start), its totals are identical to the
#     2020-2021 school-level file posted beside it, and its enrolment
#     (836,374) is a different population from the 2019-20 public table
#     (716,301). It is dated 2020-09-01.
#
#     Rates are computed here from each count over the enrolled count; the
#     printed percentages are only checked against that ratio (a column
#     misread would be off by whole points). The state row is checked
#     against the county sum, and each table must yield 67 counties.
#
#   * Exemption workbooks obtained from ADPH by request, one per grade
#     (Kindergarten, 7th, 9th), one sheet per report run, 2020-21 to 2024-25.
#     Counts and proportions of students with a full medical or religious
#     exemption and with a partial exemption while up to date. The workbook
#     does not say whether private schools are included.
#
# Both series are written to standard/data.csv.gz. `source` tells them
# apart ("survey_pdf" or "request_workbook"); 2020-21 kindergarten is in
# both, with figures that differ slightly because the workbook report was
# run on a different date. County rows have type = "county"; statewide rows
# type = "state" with geography "01". `school_type` is the population of a
# PDF table ("public and private", "public", and for the 2020-21 tables the
# published statewide breakdown into "public" and "private") and NA for the
# workbook rows.
# =============================================================================

select.state <- "AL"

sources <- read_sources()
pdf_src <- source_entry(sources, "school_entry_survey")
wb_src <- source_entry(sources, "exemption_request_workbooks")

dir.create("raw", showWarnings = FALSE)
process <- dcf::dcf_process_record()
prev <- process$fetch_state

# ---- Survey PDFs: discovery and download -------------------------------------
# The county summary files never change once posted, so a copy already in
# raw/ that validates as a PDF is not re-requested. A page that cannot be
# fetched is a warning from discover_links(); when it yields no links (that,
# or a page that no longer lists the files -- the site answers a missing
# path with HTTP 200 and its home page), the URLs recorded from earlier
# runs are used instead, which lets the on-disk files be confirmed without
# the network, and the parse proceeds on raw/ either way.
links <- discover_links(pdf_src$page_url, pdf_src$pattern, must_find = FALSE)
pdf_urls <- links$url
if (!length(pdf_urls)) {
  known <- Filter(function(r) !is.null(r$url) && grepl("\\.pdf$", r$dest), prev)
  pdf_urls <- vapply(known, function(r) r$url, character(1), USE.NAMES = FALSE)
  warning(sprintf(
    "AL: %s; using the %d URL(s) recorded in process.json from earlier runs",
    if (isTRUE(attr(links, "fetched"))) "no link on the survey page matched the pattern"
    else "the survey page could not be fetched",
    length(pdf_urls)), call. = FALSE)
}
if (length(pdf_urls)) {
  recs <- fetch_many(
    pdf_urls, dest_fn = function(u) file.path("raw", basename(u)),
    type = "pdf", if_exists = "skip", previous = prev
  )
  fetch_summary(recs, "AL survey PDFs")
  process <- record_fetch(process, recs)
} else {
  warning("AL: no survey PDF URLs known; parsing the committed raw/ copies", call. = FALSE)
}

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # ---- County summary PDFs ----------------------------------------------------
  # One entry per posted file: school-year start, grade, the population the
  # table covers, and the fields after the county name in the order printed.
  # Each count column after the enrolled count is followed by its percent of
  # enrolment ("pct_<count>"). Names used:
  #   enrolled   students enrolled
  #   valid      current (not expired) Certificate of Immunization
  #   med        Certificate of Medical Exemption; 2014-15 and 2015-16 split it
  #              into permanent (perm_med) and temporary (temp_med)
  #   part_med   medical exemption with a certificate (partial, up to date)
  #   rel        Certificate of Religious Exemption
  #   part_rel   religious exemption with a certificate
  #   expired    expired certificate
  #   no_coi     no certificate on file (absent from the 2020-21 tables)
  #   td         a Td/Tdap column only 2014-15 ("Expired Td") and 2015-16
  #              ("Tdap" up to date) carry; read and discarded
  PUBLIC_PRIVATE <- "public and private"
  layout <- function(start, cols, grade = "All grades", school_type = PUBLIC_PRIVATE) {
    stopifnot(cols[1] == "enrolled")
    rest <- cols[-1]
    list(start = start, grade = grade, school_type = school_type,
         cols = c("enrolled", as.vector(rbind(rest, paste0("pct_", rest)))))
  }
  PDF_LAYOUTS <- list(
    "2014-2015schoolentrysurvey_web1.pdf" = layout(2014L,
      c("enrolled", "valid", "perm_med", "temp_med", "rel", "part_med", "part_rel", "td", "expired", "no_coi")),
    "2015-2016schoolsurvey_web.pdf" = layout(2015L,
      c("enrolled", "valid", "perm_med", "temp_med", "rel", "part_med", "part_rel", "td", "expired", "no_coi")),
    "2016_2017_schoolsurvey.pdf" = layout(2016L,
      c("enrolled", "valid", "med", "rel", "part_med", "part_rel", "expired", "no_coi")),
    "2017-2018schoolsurvey.pdf" = layout(2017L,
      c("enrolled", "valid", "med", "rel", "part_med", "part_rel", "expired", "no_coi")),
    "2018-2019schoolsurvey.pdf" = layout(2018L,
      c("enrolled", "valid", "med", "part_med", "rel", "part_rel", "expired", "no_coi")),
    "2019-2020schoolsurvey_county.pdf" = layout(2019L,
      c("enrolled", "valid", "med", "rel", "part_med", "part_rel", "expired", "no_coi"),
      school_type = "public"),
    "2020schoolsurvey_county.pdf" = layout(2020L,
      c("enrolled", "valid", "rel", "part_rel", "med", "part_med", "expired")),
    "2020schoolsurvey_kindergartenandcounty.pdf" = layout(2020L,
      c("enrolled", "valid", "rel", "part_rel", "med", "part_med", "expired"),
      grade = "Kindergarten")
  )

  # Row labels that are not counties. District subtotals are "TOTAL" in every
  # file; the statewide row is "STATE TOTAL" (2014-15 to 2019-20), "ALL
  # COUNTIES" (2020-21 kindergarten) or "Total" (2020-21, where the "All
  # Counties" label sits on its own line above its numbers, which are then
  # repeated on the "Total" line). The 2020-21 tables end with a statewide
  # split into "Public" and "Private".
  STATE_LABELS <- c("STATE TOTAL", "ALL COUNTIES", "Total")
  SPLIT_LABELS <- c("Public", "Private")
  # In the 2020-21 tables the district name is printed on the same line as
  # its first county. Usually two or more spaces separate them and the
  # district is its own field; where the district name overflowed its column
  # ("SOUTHWESTER BALDWIN", with "N DALLAS" on the next line; "NORTHEASTERN
  # ETOWAH") only one space does, and it is part of the label.
  DISTRICT_PREFIX <- paste0(
    "^(NORTHERN|NORTHEASTERN|EAST CENTRAL|WEST CENTRAL|SOUTHWESTERN?|SOUTHEASTERN|",
    "JEFFERSON|MOBILE|N)\\s+(?=\\S)")

  # Percent printed beside a count must be that count over enrolment to
  # within 0.1 of a point (the tables round to two decimals; 2019-20 prints
  # the last column unrounded). Checked per printed line, before any merge.
  check_printed_percent <- function(d, count_cols, fn) {
    for (cc in setdiff(count_cols, "enrolled")) {
      pub <- pdf_number(d[[paste0("pct_", cc)]]) / 100
      computed <- rate_from_counts(d[[cc]], d$enrolled)
      off <- !is.na(pub) & !is.na(computed) & abs(pub - computed) > 0.001
      if (any(off)) {
        i <- which(off)[1]
        stop(sprintf(
          "AL %s: printed percent for %s disagrees with count / enrolled on %d row(s); first: %s prints %s but %d / %d = %.4f",
          fn, cc, sum(off), d$label[i], d[[paste0("pct_", cc)]][i], d[[cc]][i],
          d$enrolled[i], computed[i] * 100), call. = FALSE)
      }
    }
  }

  read_survey_pdf <- function(path) {
    fn <- basename(path)
    lay <- PDF_LAYOUTS[[fn]]
    if (is.null(lay)) {
      stop("AL: no column layout declared for ", fn,
           "; open the file, add it to PDF_LAYOUTS in ingest.R", call. = FALSE)
    }
    cols <- lay$cols
    count_cols <- cols[!startsWith(cols, "pct_")]
    pct_cols <- paste0("pct_", setdiff(count_cols, "enrolled"))

    text <- pdf_text_pages(path)
    # 2017-18 prints Excel's "#DIV/0!" in every percent cell of Bullock and
    # Macon, which reported no students. Folded to a not-reported marker so
    # the rows are kept (counts 0) and the rates are flagged "missing".
    text <- gsub("#DIV/0!", "--", text, fixed = TRUE)

    # Once with the label one field wide, once with it two fields wide for
    # the district-prefixed first county of a district. A line has one field
    # count, so no line is picked up twice.
    rows <- bind_rows(lapply(1:2, function(w) {
      r <- pdf_table_rows(path, label = "^[A-Za-z][A-Za-z .'-]*$", n_fields = length(cols),
                          col_names = cols, label_words = w, text = text)
      # pdf_table_rows() returns page and line as character when it finds
      # nothing and as integer otherwise; made uniform so the two passes bind.
      r$page <- as.integer(r$page)
      r$line <- as.integer(r$line)
      r
    })) %>%
      mutate(label = sub(DISTRICT_PREFIX, "", str_squish(label), perl = TRUE)) %>%
      filter(label != "TOTAL")
    for (cc in count_cols) rows[[cc]] <- pdf_number(rows[[cc]])
    if (anyNA(rows[count_cols])) {
      stop("AL ", fn, ": a count cell did not parse as a number", call. = FALSE)
    }
    check_printed_percent(rows, count_cols, fn)

    rows <- rows %>%
      mutate(
        role = case_when(label %in% STATE_LABELS ~ "state",
                         label %in% SPLIT_LABELS ~ "state",
                         TRUE ~ "county"),
        school_type = if_else(label %in% SPLIT_LABELS, tolower(label), lay$school_type)
      )

    # 2020-21 prints Baldwin twice ("BALDWIN", 40,353 students, then a
    # second "Baldwin" line of 132 that the district total includes). A
    # county printed on more than one line has its counts summed; the printed
    # percent then no longer describes the row and is dropped.
    counties <- rows %>%
      filter(role == "county") %>%
      group_by(label = toupper(label), role, school_type) %>%
      summarise(page = min(page),
                across(all_of(count_cols), sum),
                across(all_of(pct_cols), ~ if (n() == 1L) .x[1] else "merged"),
                n_lines = n(), .groups = "drop")
    if (any(counties$n_lines > 1L)) {
      message(sprintf("AL %s: %s printed on %d lines; counts summed", fn,
                      paste(counties$label[counties$n_lines > 1L], collapse = ", "),
                      max(counties$n_lines)))
    }
    pdf_expect_rows(counties, 67L, paste("AL", fn, "county rows"))

    states <- rows %>%
      filter(role == "state") %>%
      distinct(school_type, across(all_of(count_cols)), .keep_all = TRUE) %>%
      mutate(n_lines = 1L)
    main <- states %>% filter(school_type == lay$school_type)
    if (nrow(main) != 1L) {
      stop("AL ", fn, ": expected one statewide row, found ", nrow(main), call. = FALSE)
    }
    county_sum <- colSums(counties[count_cols])
    if (!all(county_sum == unlist(main[count_cols]))) {
      stop("AL ", fn, ": the statewide row is not the sum of the county rows", call. = FALSE)
    }
    split <- states %>% filter(school_type != lay$school_type)
    if (nrow(split) && !all(colSums(split[count_cols]) == unlist(main[count_cols]))) {
      stop("AL ", fn, ": the Public and Private rows do not add up to the statewide row",
           call. = FALSE)
    }

    d <- bind_rows(counties, states)
    # A full medical exemption is one column, or permanent plus temporary in
    # the two earliest files.
    if ("med" %in% count_cols) {
      d$full_med <- d$med
      d$flag_med <- censor_flag(d$pct_med)
    } else {
      d$full_med <- d$perm_med + d$temp_med
      d$flag_med <- mapply(function(a, b) combine_censor_flag(c(a, b)),
                           censor_flag(d$pct_perm_med), censor_flag(d$pct_temp_med))
    }
    flag_of <- function(col) {
      if (!col %in% count_cols) return(rep(NA_character_, nrow(d)))
      f <- censor_flag(d[[paste0("pct_", col)]])
      f[d$n_lines > 1L] <- CENSOR_FLAG_NONE
      f
    }
    d$flag_med[d$n_lines > 1L] <- CENSOR_FLAG_NONE
    d %>%
      transmute(
        county = label,
        type = role,
        school_type,
        time = as.Date(school_year_time(lay$start)),
        grade = lay$grade,
        N_enrolled = enrolled,
        N_valid_certificate = valid,
        flag_valid_certificate = flag_of("valid"),
        N_full_medical_exempt = full_med,
        flag_full_medical_exempt = flag_med,
        N_partial_medical_exempt_utd = part_med,
        flag_partial_medical_exempt_utd = flag_of("part_med"),
        N_full_religious_exempt = rel,
        flag_full_religious_exempt = flag_of("rel"),
        N_partial_religious_exempt_utd = part_rel,
        flag_partial_religious_exempt_utd = flag_of("part_rel"),
        N_expired_certificate = expired,
        flag_expired_certificate = flag_of("expired"),
        N_no_certificate = if ("no_coi" %in% count_cols) no_coi else NA_real_,
        flag_no_certificate = flag_of("no_coi")
      )
  }

  pdf_files <- list.files("raw", pattern = "\\.pdf$", full.names = TRUE)
  if (!length(pdf_files)) {
    stop("AL: no survey PDFs in raw/ to process.", call. = FALSE)
  }
  data_pdf <- bind_rows(lapply(pdf_files, read_survey_pdf)) %>%
    mutate(source = "survey_pdf")

  # ---- Request workbooks ------------------------------------------------------
  # ADPH publishes these as Excel-formatted proportions (0-0.04), which is
  # already the standard rate scale. The scale is declared, not inferred: the
  # previous per-element `ifelse(x > 1, x, x * 100)` rescaled only the cells at
  # or below 1, so a column containing both 0.98 and 1.63 came out with those
  # two values on different scales.
  parse_proportion <- function(x) parse_rate(x, from = "rate")

  parse_num <- function(x) {
    if (is.numeric(x)) return(as.numeric(x))
    readr::parse_number(as.character(x))
  }

  # A blank county cell marks the statewide summary row. Case and punctuation
  # are left alone here -- join_county_fips() folds those away, which the old
  # str_to_title() pass did not: it produced "St.clair" from "ST.CLAIR" and
  # then failed its own "^St\\.?\\s*Clair$" fixup on the lowercased "c".
  normalize_county_name <- function(x) {
    out <- str_squish(as.character(x))
    if_else(is.na(out) | out == "" | tolower(out) == "na", "Total", out)
  }

  infer_grade <- function(path) {
    fn <- tolower(basename(path))
    if (str_detect(fn, "kindergarten")) return("Kindergarten")
    if (str_detect(fn, "seventh"))      return("7th grade")
    if (str_detect(fn, "ninth"))        return("9th grade")
    NA_character_
  }

  # ADPH names each sheet for the date it ran the report, not for the school
  # year the report covers, so the sheet name cannot be used as `time` directly:
  # the Kindergarten workbook's third report is dated 09.14.2023 and its fourth
  # 04.05.2024, which a month-based rule would put in the same school year.
  #
  # The workbook filename carries the range instead ("...2021-2025"), and each
  # workbook holds exactly one report per school year in chronological order, so
  # the i-th sheet is the school year ending in (first year of range + i - 1).
  # The count is asserted rather than assumed -- a workbook that gains a sheet
  # breaks the build instead of shifting every year by one.
  sheet_school_years <- function(xlsx_path, sheets) {
    rng <- str_match(basename(xlsx_path), "(\\d{4})\\s*-\\s*(\\d{4})")
    if (is.na(rng[1, 1])) {
      stop("No YYYY-YYYY school-year range in filename: ", basename(xlsx_path))
    }
    end_years <- as.integer(rng[1, 2]):as.integer(rng[1, 3])
    if (length(sheets) != length(end_years)) {
      stop(sprintf(
        paste0("%s: %d sheet(s) but %d school year(s) in the filename range ",
               "%s-%s. The sheet-to-school-year mapping below assumes one ",
               "report per year -- check the workbook before changing it."),
        basename(xlsx_path), length(sheets), length(end_years),
        rng[1, 2], rng[1, 3]))
    }
    dates <- as.Date(sheets, format = "%m.%d.%Y")
    if (any(is.na(dates))) {
      stop("Sheet name is not mm.dd.yyyy: '",
           paste(sheets[is.na(dates)], collapse = "', '"), "' in ",
           basename(xlsx_path))
    }
    setNames(school_year_time_from_end(end_years[rank(dates)]), sheets)
  }

  process_one_workbook <- function(xlsx_path) {
    grade_label <- infer_grade(xlsx_path)
    if (is.na(grade_label)) stop("Could not infer grade from filename: ", basename(xlsx_path))

    sheets <- readxl::excel_sheets(xlsx_path)
    sheet_time <- sheet_school_years(xlsx_path, sheets)

    bind_rows(lapply(sheets, function(sh) {
      df <- readxl::read_excel(xlsx_path, sheet = sh)

      # Normalize headers (remove embedded line breaks / tabs)
      names(df) <- names(df) %>%
        str_replace_all("\\s+", " ") %>%
        str_trim()

      df %>%
        transmute(
          time = as.Date(sheet_time[[sh]]),
          county = normalize_county_name(County),
          grade = grade_label,

          # The denominator of every percentage in the workbook: each published
          # "%" column equals its own "#" column divided by this one, to machine
          # precision, across every county and sheet. So this is the enrolment
          # count the rates are taken over, and it is carried through as
          # N_enrolled rather than dropped.
          N_enrolled = parse_num(`# of Students`),

          N_full_medical_exempt = parse_num(`# with Full Medical Exemption`),
          N_partial_medical_exempt_utd = parse_num(`# UTD with Partial Medical Exemption`),
          N_full_religious_exempt = parse_num(`# with Full Religous Exemption`),
          N_partial_religious_exempt_utd = parse_num(`# UTD with Partial Religious Exemption`),

          rate_full_medical_exempt = parse_proportion(`% with Full Medical Exemption`),
          rate_partial_medical_exempt_utd = parse_proportion(`% UTD with Partial Medical Exemtion`),
          rate_full_religious_exempt = parse_proportion(`% with Full Religous Exemption`),
          rate_partial_religious_exempt_utd = parse_proportion(`% UTD with Partial Religious Exemption`)
        )
    }))
  }

  # "~$..." files are Excel's lock files, present only while a workbook is
  # open in Excel; raw_state_md5() leaves them out of the change detection
  # for the same reason.
  xlsx_files <- list.files("raw", "\\.xlsx$", recursive = TRUE, full.names = TRUE)
  xlsx_files <- xlsx_files[!grepl("^~\\$", basename(xlsx_files))]
  if (!length(xlsx_files)) {
    stop("AL: no exemption workbooks in raw/ to process.", call. = FALSE)
  }

  data_wb <- bind_rows(lapply(xlsx_files, process_one_workbook)) %>%
    mutate(type = if_else(county == "Total", "state", "county"),
           school_type = NA_character_,
           source = "request_workbook")

  # ---- Assemble ---------------------------------------------------------------
  # Rates for the PDF rows are each count over N_enrolled (NA where a county
  # reported no students). For the workbook rows the published proportions
  # are kept; they equal count / N_enrolled to machine precision.
  data_all <- bind_rows(data_pdf, data_wb) %>%
    join_county_fips("AL", statewide = c(STATE_LABELS, SPLIT_LABELS)) %>%
    mutate(
      geography_name = if_else(type == "state", "Alabama", geography_name),
      rate_valid_certificate = rate_from_counts(N_valid_certificate, N_enrolled),
      rate_expired_certificate = rate_from_counts(N_expired_certificate, N_enrolled),
      rate_no_certificate = rate_from_counts(N_no_certificate, N_enrolled),
      rate_full_medical_exempt = coalesce(rate_full_medical_exempt,
                                          rate_from_counts(N_full_medical_exempt, N_enrolled)),
      rate_partial_medical_exempt_utd = coalesce(rate_partial_medical_exempt_utd,
                                                 rate_from_counts(N_partial_medical_exempt_utd, N_enrolled)),
      rate_full_religious_exempt = coalesce(rate_full_religious_exempt,
                                            rate_from_counts(N_full_religious_exempt, N_enrolled)),
      rate_partial_religious_exempt_utd = coalesce(rate_partial_religious_exempt_utd,
                                                   rate_from_counts(N_partial_religious_exempt_utd, N_enrolled)),

      # Canonical exemption columns. ADPH publishes two exemption grounds,
      # medical and religious, each split into a full exemption and a partial
      # "with certificate" status. The canonical columns take the FULL
      # exemption of each ground; a student who is up to date under a partial
      # exemption is not exempt from the schedule.
      N_medical_exempt = N_full_medical_exempt,
      rate_medical_exempt = rate_full_medical_exempt,
      N_religious_exempt = N_full_religious_exempt,
      rate_religious_exempt = rate_full_religious_exempt,

      # Total fully exempt, from the two grounds. ADPH tabulates full medical
      # and full religious as separate grounds for the same student
      # population, so they add. The share is divided out of the counts
      # rather than taken from a published total (there is none).
      N_full_exempt = N_full_medical_exempt + N_full_religious_exempt,
      rate_full_exempt = rate_from_counts(N_full_exempt, N_enrolled)

      # NOT emitted: N_personal_exempt / rate_personal_exempt. Alabama has no
      # personal or philosophical exemption -- only medical and religious.
    ) %>%
    # No rounding here: these are rates, so round(x, 2) collapsed every value
    # below 0.005 to 0 -- which is most of the column, since ADPH exemption
    # rates run 0-0.04. Source precision is kept instead.
    select(
      time, geography, geography_name, type, grade, school_type, source,
      N_enrolled,
      N_valid_certificate, rate_valid_certificate, flag_valid_certificate,
      N_medical_exempt, rate_medical_exempt,
      N_religious_exempt, rate_religious_exempt,
      N_full_exempt, rate_full_exempt,
      N_full_medical_exempt, rate_full_medical_exempt, flag_full_medical_exempt,
      N_partial_medical_exempt_utd, rate_partial_medical_exempt_utd, flag_partial_medical_exempt_utd,
      N_full_religious_exempt, rate_full_religious_exempt, flag_full_religious_exempt,
      N_partial_religious_exempt_utd, rate_partial_religious_exempt_utd, flag_partial_religious_exempt_utd,
      N_expired_certificate, rate_expired_certificate, flag_expired_certificate,
      N_no_certificate, rate_no_certificate, flag_no_certificate
    ) %>%
    arrange(time, grade, source, school_type,
            factor(type, levels = c("state", "county")), geography)

  message(sprintf(
    "AL: %d survey PDF rows (%d county, %d state) for %d school years; %d workbook rows for %d school years",
    sum(data_all$source == "survey_pdf"),
    sum(data_all$source == "survey_pdf" & data_all$type == "county"),
    sum(data_all$source == "survey_pdf" & data_all$type == "state"),
    n_distinct(data_all$time[data_all$source == "survey_pdf"]),
    sum(data_all$source == "request_workbook"),
    n_distinct(data_all$time[data_all$source == "request_workbook"])))

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(data_all, "Alabama", "./standard/data.csv.gz", from = "rate")
  update_latest_year(latest_school_year(out[out$source == "survey_pdf", ]),
                     ids = pdf_src$id)
  update_latest_year(latest_school_year(out[out$source == "request_workbook", ]),
                     ids = wb_src$id)

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
