library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/county_fips.R")
source("../../resources/school_year.R")

# =============================================================================
# CA - Kindergarten and 7th-grade school immunization assessment, by county
#
# Every row here comes from a CDPH "Report Tables" workbook -- the county tables
# CDPH publishes alongside each year's assessment summary. They are CDPH's own
# county aggregates, not roll-ups of school-level data, so the whole series is
# one method. Two cohorts, from these sources:
#
#   kindergarten / 1st grade
#     raw/California Vaccine Exemption.xlsx  "Kindergarten by county 19-23"
#                                            KG 2019-20..2022-23, 1st 2022-23
#     raw/CA_KG_cdph_2023-24.xlsx  Table 2   KG 2023-24
#     raw/CA_KG_cdph_2024-25.xlsx  Table 2   KG 2024-25
#     -- per-vaccine coverage (DTaP, polio, MMR, hepatitis B, 2+ varicella),
#        a second table CDPH publishes alongside the one above:
#     raw/CA_KG_cdph_2022-23.xlsx  Table 4   KG 2019-20..2022-23, 1st 2022-23
#     raw/CA_KG_cdph_2023-24.xlsx  Table 3   KG 2023-24
#     raw/CA_KG_cdph_2024-25.xlsx  Table 2   KG 2024-25 (columns 8-12 of the
#                                            same sheet used for the row above)
#
#   7th / 8th grade
#     raw/California Vaccine Exemption.xlsx  "7th grade 19-22 (Tdap)"
#                                            "7th grade 19-22 (Varicella)"
#                                            7th 2019-20..2021-22, 8th 2021-22
#     raw/CA_7th_cdph_2019-20.xlsx Table 3   7th 2018-19, Tdap only
#
# Following the convention in data/CT/ingest.R, the two cohorts stack into ONE
# canonical file, standard/data.csv.gz -- the name
# scripts/build_all_states_county_standard.R and scripts/generate_measure_info.R
# both read by -- with two narrower files beside it that each drop the other
# cohort's structurally empty columns:
#
#   standard/data.csv.gz               both cohorts, every measure column
#   standard/data_kindergarten.csv.gz  kindergarten / 1st grade only
#   standard/data_grade7.csv.gz        7th / 8th grade only
#
# The 2019-20 rows of the 7th-grade report duplicate the workbook exactly (same
# enrolment, same rates to the digit), so only its 2018-19 rows are taken. CDPH
# published no 7th-grade varicella county table for 2018-19 -- that year's
# varicella columns are empty rather than filled from another source.
#
# CDPH stopped publishing 7th-grade report tables after the 2020-22 round, so
# 2021-22 is the last 7th-grade year available at county level.
#
# NOT USED: the CHHS Open Data school-level files, which reach back to 2013-14.
# Rolling those up to county does not reproduce these tables. Enrolment sums
# exactly, but the school-level PERCENT column is integer-rounded (0-99) and the
# COUNT column is suppressed on two thirds of rows, so a county rate rebuilt
# from them lands a median 0.5-0.8 percentage points from the published figure
# and up to 5 points out in the small counties -- against quantities that are
# themselves usually under 3 percent. Splicing that onto this series would show
# up as trend that is not there.
#
# The 7th-grade tables are one table per vaccine over the same students, so they
# are joined on (county, school year, grade) and the measures carry a _tdap /
# _varicella suffix. Long-over-vaccine is not an option: the national build
# excludes any state whose rows are long over a measure stratum (LONG_STRATA).
#
# Published as proportions (0.9607, not 96.07), hence from = "rate".
#
# CDPH de-identifies small jurisdictions two different ways, and the output
# treats them differently because they carry different amounts of information:
#
#   BOUNDED   the cell states a limit -- "<35" students, "<=5.0%" overdue,
#             ">=95.0%" up to date. That is a fact about the county, so the
#             column keeps the bound as its value and flag_<measure> records
#             "bottom_coded" or "top_coded". Reading such a value as a
#             measurement overstates it; the flag is what says so.
#   WITHHELD  the cell states nothing -- "N/A", "--", "---", printed where the
#             jurisdiction has under 20 enrollees (35 for 7th grade). The value
#             is empty and the flag is "suppressed". Nothing is invented for it.
#
# Flags are per measure, not per row, because a row is rarely censored as a
# whole: one county can have an overdue rate printed as "<=5.0%" beside four
# figures CDPH published outright.
# =============================================================================

workbook <- "raw/California Vaccine Exemption.xlsx"

# ---- Download the CDPH report tables ----------------------------------------
#
# The workbook is supplied by hand and has no URL, so it is left alone. The
# report tables are downloaded to a temporary path and only moved into place
# when the new copy is at least as large as the one on disk: CDPH occasionally
# serves a truncated or WAF-blocked response, which would otherwise silently
# replace a good file with a stub and drop whole school years from the output.
report_urls <- c(
  "raw/CA_KG_cdph_2024-25.xlsx" =
    "https://www.cdph.ca.gov/Programs/CID/DCDC/CDPH%20Document%20Library/Immunization/2024-25KindergartenReport.xlsx",
  "raw/CA_KG_cdph_2023-24.xlsx" =
    "https://www.cdph.ca.gov/Programs/CID/DCDC/CDPH%20Document%20Library/Immunization/2023-24GradeKReport.xlsx",
  # Downloaded only for its Table 4 (per-vaccine coverage) -- its Table 3 is
  # the same 2019-20..2022-23 admission-status data already taken from the
  # hand-supplied workbook, and is not used again from here.
  "raw/CA_KG_cdph_2022-23.xlsx" =
    "https://www.cdph.ca.gov/Programs/CID/DCDC/CDPH%20Document%20Library/Immunization/2022-23GradeKReport.xlsx",
  "raw/CA_7th_cdph_2019-20.xlsx" =
    "https://www.cdph.ca.gov/Programs/CID/DCDC/CDPH%20Document%20Library/Immunization/2020SchoolAssessmentReport.xlsx"
)

dir.create("raw", showWarnings = FALSE)
for (dest in names(report_urls)) {
  tmp <- paste0(dest, ".part")
  ok <- tryCatch({
    download.file(report_urls[[dest]], tmp, mode = "wb", quiet = TRUE)
    TRUE
  }, error = function(e) FALSE, warning = function(w) FALSE)

  if (ok && file.exists(tmp)) {
    old <- if (file.exists(dest)) file.size(dest) else 0
    if (file.size(tmp) >= old) {
      file.rename(tmp, dest)
    } else {
      warning(sprintf(
        "kept existing %s: download gave %.0f kB against %.0f kB on disk",
        basename(dest), file.size(tmp) / 1e3, old / 1e3), call. = FALSE)
    }
  }
  unlink(tmp)
}

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # ---- Report-table reader --------------------------------------------------
  #
  # Read positionally rather than by header, because the header is not one row:
  # the workbook's kindergarten sheet titles its columns on sheet row 2, its
  # 7th-grade sheets split theirs across rows 4 and 5 ("Total Students" on 4,
  # "Number" under it on 5), and the standalone report tables put theirs on
  # row 2. read_excel() can only take one row as names, so half the names come
  # back blank whatever is chosen. `cols` names the leading columns in order;
  # any beyond it are dropped (the 2024-25 table appends five per-vaccine
  # coverage columns this file does not carry).
  #
  # Every cell is read as text, because the value columns are not numeric: a
  # county too small to report reads "N/A*", "---*" or "<35*", and a top-coded
  # one reads ">=99.5%" or "<=0.5%". Read as numbers they arrive as a
  # failure indistinguishable from a parser bug; read as text they go through
  # resources/rate_scale.R, which recognises each marker and returns NA for it
  # deliberately.
  MEASURES <- c("N_enrolled", "pct_up_to_date", "pct_conditional", "pct_pme",
                "pct_other_lacking", "pct_overdue")

  read_report_table <- function(path, sheet, skip, cols) {
    d <- readxl::read_excel(path, sheet = sheet, skip = skip, col_names = FALSE,
                            col_types = "text", .name_repair = "minimal")
    if (ncol(d) < length(cols)) {
      stop(sprintf(
        "%s / %s has %d columns, expected at least %d -- the layout changed",
        basename(path), sheet, ncol(d), length(cols)), call. = FALSE)
    }
    d <- d[, seq_along(cols)]
    names(d) <- cols

    # Every one of these tables trails footnote prose in the county column, and
    # the workbook's 7th-grade sheets lead with a leftover second header row.
    # No California county name runs past 30 characters or opens with a
    # footnote marker, so this drops the prose -- including the wrapped
    # continuation lines -- without touching a geography. Anything that still
    # slips through is not silently absorbed: join_county_fips() stops on a
    # label it cannot account for.
    lab <- str_squish(d$county)
    d <- d[!is.na(lab) & lab != "" & nchar(lab) <= 30 &
             !str_detect(lab, "^[*^\u2020]"), ]

    # Trailing footnote markers on a value cell: "N/A*", "N/A**", "--**",
    # "---*", "<35*". The marker is the footnote reference, not part of the
    # value, and it stops resources/rate_scale.R from recognising the cell as
    # censored ("n/?a" and "-{2,}" are anchored, so "N/A*" matches neither).
    # Stripped here so is_censored() sees the canonical "N/A" / "--" / "<35"
    # and clean_numeric() blanks the cell on purpose rather than by failing to
    # coerce it. An asterisk never appears on a genuine number.
    #
    # Matched by NAME PATTERN (pct_/N_), not against a fixed measure list, so
    # a table with columns this reader has never seen -- the per-vaccine
    # coverage columns below -- gets the same treatment automatically. Missing
    # a column here would not error: clean_numeric() still returns NA for an
    # unstripped "N/A*" (as.numeric() of it is NA regardless), but
    # censor_direction() would not recognise the marker, and the cell would
    # come back as an ordinary empty value with no flag explaining it.
    val <- grep("^(pct_|N_)", names(d), value = TRUE)
    d <- d %>% mutate(across(all_of(val), ~ trimws(sub("\\*+\\s*$", "", .x))))

    # The workbook's kindergarten sheet repeats Yuba 2019-20 verbatim -- same
    # enrolment, same five percentages. Deduplicated on the full row, so a
    # repeat that DISAGREED would survive to the duplicate-key check below
    # instead of one copy being picked arbitrarily.
    dplyr::distinct(d)
  }

  # ---- Shared shaping -------------------------------------------------------
  GRADE_LABELS <- c(
    "Kindergarten" = "Kindergarten", "1st Grade" = "1st grade",
    "7" = "7th grade", "8" = "8th grade"
  )

  label_grade <- function(x) {
    g <- trimws(as.character(x))
    # The workbook's 7th-grade sheets store the grade as a NUMBER, so reading
    # the sheet as text renders it "7.0"; the kindergarten sheet uses a string.
    g <- sub("\\.0+$", "", g)
    out <- unname(GRADE_LABELS[g])
    if (any(is.na(out))) {
      stop("unrecognised grade label(s): ",
           paste(unique(g[is.na(out)]), collapse = ", "), call. = FALSE)
    }
    out
  }

  # A school-year label is "2019-20" in the workbook and the 7th-grade report.
  keep_school_years <- function(d) {
    d[!is.na(d$school_year) & grepl("^\\d{4}-\\d{2}$", d$school_year), ]
  }

  # Statewide rows are declared so a county spelling CDPH changes surfaces as an
  # error instead of being folded into the state total. The 2019-20 7th-grade
  # report sets its labels in upper case, which county_fips_key() folds away.
  attach_fips <- function(df) {
    df %>%
      mutate(county = str_squish(county)) %>%
      join_county_fips("CA", statewide = "State Total")
  }

  # Why one cell has no number, as a flag column beside the value it explains.
  #
  #   ""              the source printed a number -- nothing to report
  #   "suppressed"    CDPH withheld it to protect a small jurisdiction, giving
  #                   no bound ("N/A", "--", "---")
  #   "top_coded"     the true value is AT OR ABOVE a printed bound (">=95.0%",
  #                   ">=99.5%") -- CDPH does this for jurisdictions of 20-49,
  #                   50-99 and 100-499 enrollees at 95, 98 and 99 percent
  #   "bottom_coded"  the true value is AT OR BELOW one ("<=0.5%", "<35")
  #   NA              the source cell was empty -- no marker and no number
  #
  # Built on censor_direction() rather than rate_scale.R's own censor_flag()
  # because that function splits withheld cells into "suppressed" (an asterisk
  # run) and "missing" ("N/A", "--"), a distinction drawn from how other states
  # mark a cell. CDPH writes "N/A" and "---" for exactly what its own footnote
  # calls omission for de-identification, so on this source both are
  # suppression and calling them "missing" would say the data was never
  # collected.
  #
  # "" and NA both serialise to an empty CSV field, so only the exceptions
  # carry text: a flag column that spells out "no" 600 times hides the handful
  # of rows that need attention.
  censor_label <- function(x) {
    chr <- trimws(as.character(x))
    dir <- censor_direction(chr)
    out <- ifelse(dir == "right", "top_coded",
                  ifelse(dir == "left", "bottom_coded",
                         ifelse(dir == "undirected", "suppressed", "")))
    out[is.na(dir)] <- ""
    out[is.na(chr) | chr == ""] <- NA_character_
    out
  }

  # The number a cell states, keeping the BOUND where CDPH printed one instead
  # of a measurement. Alpine's 7th-grade enrolment reads "<35", which is not an
  # absence of information -- it says the county had at most 35 students in the
  # grade -- so the column carries 35 and flag_enrolled carries "bottom_coded".
  # Only a withheld cell ("N/A", "--", "---"), which states no bound at all,
  # stays empty.
  #
  # `from` is not a formality. CDPH writes an ordinary cell as an Excel
  # proportion (0.0162) and a bounded one as a literal percent string
  # ("<=1.0%"), so the two scales sit in the SAME column and the bound alone
  # has to be divided by 100. Passing from = "rate" for a rate column would put
  # 1.0 where 0.01 belongs. Counts are the other way round: "<35" is already a
  # count, so they take from = "rate".
  #
  # clean_numeric() returns NA for every censored cell and censor_bound()
  # returns a number for none but the bounded ones, so the two never disagree
  # about a cell and coalesce() simply picks whichever spoke.
  resolve_value <- function(x, from) {
    dplyr::coalesce(clean_numeric(x), censor_bound(x, from = from))
  }

  # ---- Kindergarten and 1st grade --------------------------------------------
  KG_COLS <- c("county", "N_enrolled", "pct_up_to_date", "pct_conditional",
               "pct_pme", "pct_other_lacking", "pct_overdue")

  kg_workbook <- read_report_table(
    workbook, "Kindergarten by county 19-23", skip = 2,
    cols = c("state", "county", "school_year", "grade", KG_COLS[-1])
  ) %>% keep_school_years()

  # The standalone kindergarten tables cover one year and one grade, neither of
  # which is a column in them -- both are in the sheet title. Supplied here.
  kg_reports <- bind_rows(
    read_report_table("raw/CA_KG_cdph_2023-24.xlsx", "Table 2", skip = 2,
                      cols = KG_COLS) %>%
      mutate(school_year = "2023-24", grade = "Kindergarten"),
    read_report_table("raw/CA_KG_cdph_2024-25.xlsx", "Table 2", skip = 2,
                      cols = KG_COLS) %>%
      mutate(school_year = "2024-25", grade = "Kindergarten")
  )

  # Flags are added in a separate step, ahead of the transmute() that converts
  # the value columns, because dplyr evaluates an expression list in order: a
  # flag written alongside `N_enrolled = clean_numeric(N_enrolled)` would read
  # the number that line had just produced instead of the marker it replaced,
  # and every flag would come back empty.
  add_flags <- function(d, cols) {
    for (cc in cols) d[[sub("^(pct|N)_", "flag_", cc)]] <- censor_label(d[[cc]])
    d
  }

  KG_MEASURES <- c("N_enrolled", "pct_conditional", "pct_pme",
                   "pct_other_lacking", "pct_overdue")

  kg <- bind_rows(kg_workbook, kg_reports) %>%
    attach_fips() %>%
    add_flags(KG_MEASURES) %>%
    transmute(
      geography, geography_name,
      time = school_year_time(str_sub(school_year, 1, 4)),
      grade = label_grade(grade),
      N_enrolled = resolve_value(N_enrolled, "rate"), flag_enrolled,
      pct_conditional = resolve_value(pct_conditional, "percent"),
      flag_conditional,
      pct_pme = resolve_value(pct_pme, "percent"), flag_pme,
      pct_other_lacking = resolve_value(pct_other_lacking, "percent"),
      flag_other_lacking,
      pct_overdue = resolve_value(pct_overdue, "percent"), flag_overdue
    )

  # ---- Kindergarten and 1st grade: per-vaccine coverage ----------------------
  #
  # CDPH publishes a SECOND county table alongside the admission-status one
  # above: coverage by antigen (4+ DTaP, 3+ polio, 2+ MMR, 3+ hepatitis B,
  # 2+ varicella) rather than by admission category. Joined onto `kg` by
  # (geography, time, grade) rather than merged at the row-reading stage,
  # because it comes from a different sheet in every year and, for 2019-20
  # through 2022-23, a different WORKBOOK than the admission-status figures for
  # the same years.
  KG_VAX_MEASURES <- c("pct_dtap", "pct_polio", "pct_mmr", "pct_hep_b",
                       "pct_varicella")
  KG_VAX_COLS_YEAR <- c("county", "N_enrolled", KG_VAX_MEASURES)

  # CDPH's own 2022-23 workbook repeats Yuba's 2019-20 row twice, at two
  # different roundings (18 significant digits against a flat 3) -- a paste
  # error in the source, not a second observation. Every other row in the
  # table carries full precision, so the row that happens to equal its OWN
  # 3-decimal rounding on every one of the five measures at once is the one to
  # drop; a genuinely measured county landing on an exact 3-decimal value in
  # all five columns simultaneously is not a realistic coincidence. Where that
  # test does not leave exactly one row per key -- neither row is the clean
  # one, or more than one duplicate survives -- this stops rather than
  # guessing, the same way the duplicate-key check at the end of this script
  # does for every other frame.
  drop_rounded_duplicate <- function(d, measure_cols) {
    key <- c("county", "school_year", "grade")
    dup <- duplicated(d[key]) | duplicated(d[key], fromLast = TRUE)
    if (!any(dup)) return(d)

    v <- as.data.frame(lapply(d[measure_cols], clean_numeric))
    is_rounded <- apply(v, 1, function(row) {
      row <- row[!is.na(row)]
      length(row) > 0 && all(abs(row - round(row, 3)) < 1e-9)
    })

    keep <- rep(TRUE, nrow(d))
    for (k in unique(do.call(paste, d[dup, key]))) {
      idx <- which(do.call(paste, d[key]) == k)
      if (sum(!is_rounded[idx]) != 1) {
        stop(sprintf(paste0("CA kindergarten vaccine table: '%s' has %d ",
                            "duplicate row(s) that are not one clean ",
                            "3-decimal repeat of one full-precision row -- ",
                            "resolve by hand"), k, length(idx)), call. = FALSE)
      }
      keep[idx[is_rounded[idx]]] <- FALSE
    }
    d[keep, , drop = FALSE]
  }

  kg_vax <- bind_rows(
    read_report_table(
      "raw/CA_KG_cdph_2022-23.xlsx", "Table 4", skip = 2,
      cols = c("county", "school_year", "grade", KG_VAX_COLS_YEAR[-1])
    ) %>% keep_school_years() %>% drop_rounded_duplicate(KG_VAX_MEASURES),
    read_report_table("raw/CA_KG_cdph_2023-24.xlsx", "Table 3", skip = 2,
                      cols = KG_VAX_COLS_YEAR) %>%
      mutate(school_year = "2023-24", grade = "Kindergarten"),
    # Table 2 of the 2024-25 workbook is the SAME sheet already read above for
    # admission status, twelve columns wide; the last five are these vaccine
    # measures. Read again with all twelve named, then keep only the ones
    # this block needs -- read_report_table() otherwise keeps only a LEADING
    # run of named columns, and county/N_enrolled are not adjacent to them.
    read_report_table(
      "raw/CA_KG_cdph_2024-25.xlsx", "Table 2", skip = 2,
      cols = c("county", "N_enrolled", "pct_up_to_date", "pct_conditional",
              "pct_pme", "pct_other_lacking", "pct_overdue", KG_VAX_MEASURES)
    ) %>%
      select(all_of(KG_VAX_COLS_YEAR)) %>%
      mutate(school_year = "2024-25", grade = "Kindergarten")
  ) %>%
    attach_fips() %>%
    add_flags(KG_VAX_MEASURES) %>%
    transmute(
      geography,
      time = school_year_time(str_sub(school_year, 1, 4)),
      grade = label_grade(grade),
      # Kept only to be checked against `kg`'s own N_enrolled just below, not
      # carried into the output -- one enrolment column per file is enough.
      N_enrolled_vax_table = clean_numeric(N_enrolled),
      pct_dtap = resolve_value(pct_dtap, "percent"), flag_dtap,
      pct_polio = resolve_value(pct_polio, "percent"), flag_polio,
      pct_mmr = resolve_value(pct_mmr, "percent"), flag_mmr,
      pct_hep_b = resolve_value(pct_hep_b, "percent"), flag_hep_b,
      pct_varicella = resolve_value(pct_varicella, "percent"), flag_varicella
    )

  kg <- kg %>% left_join(kg_vax, by = c("geography", "time", "grade"))

  # The vaccine table publishes its own enrolment. Where both it and the
  # admission-status table report one for the same (geography, time, grade),
  # they have to agree -- a gap would mean the two tables are not actually
  # describing the same students, and joining their measures onto one row
  # would be wrong.
  enr_gap <- kg %>%
    filter(!is.na(N_enrolled), !is.na(N_enrolled_vax_table),
           N_enrolled != N_enrolled_vax_table)
  if (nrow(enr_gap)) {
    stop(sprintf(paste0("CA kindergarten: the admission-status and ",
                        "per-vaccine tables disagree on enrolment in %d ",
                        "row(s); first: %s %s grade %s, %s vs %s"),
                 nrow(enr_gap), enr_gap$geography[1], enr_gap$time[1],
                 enr_gap$grade[1], enr_gap$N_enrolled[1],
                 enr_gap$N_enrolled_vax_table[1]), call. = FALSE)
  }
  kg$N_enrolled_vax_table <- NULL

  # ---- 7th and 8th grade -----------------------------------------------------
  #
  # Two tables per year, one per vaccine, over the same students. Joined on the
  # key rather than bound by position, and the enrolment agreement is then
  # checked where both tables report one -- if they ever disagree on a
  # denominator they are not the same students and suffixing the measures onto a
  # single row would be wrong.
  G7_WORKBOOK_COLS <- c("state", "county", "school_year", "grade",
                        "N_enrolled", "pct_up_to_date", "pct_conditional",
                        "pct_pme", "pct_other_lacking", "pct_overdue")

  g7_tdap <- bind_rows(
    read_report_table(workbook, "7th grade 19-22 (Tdap)", skip = 5,
                      cols = G7_WORKBOOK_COLS) %>% keep_school_years(),
    # The 2019-20 report carries both 2018-19 and 2019-20; its 2019-20 rows are
    # the workbook's rows to the digit, so taking them too would duplicate the
    # key. Grade is not a column -- it is the 7th-grade report throughout.
    read_report_table(
      "raw/CA_7th_cdph_2019-20.xlsx", "Table 3", skip = 2,
      cols = c("county", "school_year", "N_enrolled", "pct_up_to_date",
               "pct_conditional", "pct_pme", "pct_other_lacking", "pct_overdue")
    ) %>% keep_school_years() %>% filter(school_year == "2018-19") %>%
      mutate(grade = "7")
  )

  g7_varicella <- read_report_table(workbook, "7th grade 19-22 (Varicella)",
                                    skip = 5, cols = G7_WORKBOOK_COLS) %>%
    keep_school_years()

  suffix_measures <- function(d, vax) {
    d %>%
      select(county, school_year, grade,
             all_of(setdiff(MEASURES, "pct_up_to_date"))) %>%
      rename_with(~ paste0(.x, "_", vax), -c(county, school_year, grade))
  }

  g7 <- full_join(suffix_measures(g7_tdap, "tdap"),
                  suffix_measures(g7_varicella, "varicella"),
                  by = c("county", "school_year", "grade"))

  enr_gap <- g7 %>%
    mutate(t = clean_numeric(N_enrolled_tdap),
           v = clean_numeric(N_enrolled_varicella)) %>%
    filter(!is.na(t), !is.na(v), t != v)
  if (nrow(enr_gap)) {
    stop(sprintf(paste0("CA 7th grade: the Tdap and varicella tables disagree ",
                        "on enrolment in %d row(s); first: %s %s grade %s, ",
                        "%s vs %s"),
                 nrow(enr_gap), enr_gap$county[1], enr_gap$school_year[1],
                 enr_gap$grade[1], enr_gap$N_enrolled_tdap[1],
                 enr_gap$N_enrolled_varicella[1]), call. = FALSE)
  }

  G7_MEASURES <- as.vector(outer(
    c("pct_conditional", "pct_pme", "pct_other_lacking", "pct_overdue"),
    c("tdap", "varicella"), paste, sep = "_"))

  g7 <- g7 %>%
    attach_fips() %>%
    # One denominator, checked equal above. The Tdap table is preferred and
    # varicella used only where the Tdap cell is absent -- 2018-19 has no
    # varicella table, and a future year could arrive the other way round. The
    # choice is made on the RAW cell so the value and its flag always describe
    # the same source cell.
    mutate(
      raw_enrolled = coalesce(na_if(str_squish(N_enrolled_tdap), ""),
                              na_if(str_squish(N_enrolled_varicella), "")),
      flag_enrolled = censor_label(raw_enrolled)
    ) %>%
    add_flags(G7_MEASURES) %>%
    transmute(
      geography, geography_name,
      time = school_year_time(str_sub(school_year, 1, 4)),
      grade = label_grade(grade),
      N_enrolled = resolve_value(raw_enrolled, "rate"), flag_enrolled,
      pct_conditional_tdap = resolve_value(pct_conditional_tdap, "percent"),
      flag_conditional_tdap,
      pct_pme_tdap = resolve_value(pct_pme_tdap, "percent"), flag_pme_tdap,
      pct_other_lacking_tdap = resolve_value(pct_other_lacking_tdap, "percent"),
      flag_other_lacking_tdap,
      pct_overdue_tdap = resolve_value(pct_overdue_tdap, "percent"),
      flag_overdue_tdap,
      pct_conditional_varicella =
        resolve_value(pct_conditional_varicella, "percent"),
      flag_conditional_varicella,
      pct_pme_varicella = resolve_value(pct_pme_varicella, "percent"),
      flag_pme_varicella,
      pct_other_lacking_varicella =
        resolve_value(pct_other_lacking_varicella, "percent"),
      flag_other_lacking_varicella,
      pct_overdue_varicella = resolve_value(pct_overdue_varicella, "percent"),
      flag_overdue_varicella
    )

  # ---- Keep the county rows, combine, and write ------------------------------
  #
  # The statewide rows are dropped rather than never parsed, so that a
  # "State Total" label CDPH stops using still fails the join_county_fips()
  # check above instead of vanishing from the counts below.
  county_rows <- function(d, label) {
    d <- d %>% arrange(time, grade, geography)

    key <- c("geography", "time", "grade")
    dup <- d[duplicated(d[key]), ]
    if (nrow(dup)) {
      stop(sprintf("CA %s: %d duplicate (%s) row(s); first: %s %s %s",
                   label, nrow(dup), paste(key, collapse = ", "),
                   dup$geography[1], dup$time[1], dup$grade[1]), call. = FALSE)
    }

    n_state <- sum(nchar(d$geography) == 2)
    county <- d %>% filter(nchar(geography) == 5)
    if (!nrow(county) || !n_state) {
      stop(sprintf("CA %s: expected both levels, got %d county and %d state",
                   label, nrow(county), n_state), call. = FALSE)
    }
    message(sprintf(
      "California %s: %d county row(s), %d counties, %s (%d statewide dropped)",
      label, nrow(county), length(unique(county$geography)),
      paste(range(county$time), collapse = " to "), n_state))
    county
  }

  # One combined file, following the CT convention: both cohorts stack into
  # standard/data.csv.gz on the shared (geography, time, grade) key, with two
  # narrower files beside it that each drop the other cohort's structurally
  # empty columns.
  #
  # data.csv.gz has to be the ALL-COHORT file rather than either cohort alone,
  # because scripts/build_all_states_county_standard.R and
  # scripts/generate_measure_info.R both read data.csv.gz by that exact name --
  # naming the union anything else would drop whichever cohort is not in it out
  # of the national pipeline. A kindergarten row carries 16 empty 7th-grade
  # columns (its four Tdap and four varicella rate/flag pairs) and a 7th-grade
  # row carries 18 empty kindergarten ones (four admission-status and five
  # per-vaccine rate/flag pairs); that is the price of one canonical file, and
  # the cohort keys never collide (Kindergarten/1st grade vs. 7th/8th grade),
  # so stacking them loses nothing a consumer filtering on `grade` cannot
  # recover.
  KG_GRADES <- c("Kindergarten", "1st grade")
  G7_GRADES <- c("7th grade", "8th grade")

  combined <- bind_rows(county_rows(kg, "kindergarten"),
                        county_rows(g7, "7th grade")) %>%
    arrange(time, grade, geography)
  print(table(combined$grade, substr(combined$time, 1, 4)))

  # write_standard() converts pct_* -> rate_*, canonicalises count names, and
  # drops any column that is empty across the WHOLE combined file -- none are,
  # since every kindergarten measure is populated by kindergarten rows and
  # every 7th-grade measure by 7th-grade rows. The returned frame is reused for
  # the two narrower files below rather than rebuilt from `kg`/`g7`, so all
  # three files are guaranteed to agree on every value they share.
  wide <- write_standard(combined, "California", "./standard/data.csv.gz",
                         from = "rate")

  # `from = "rate"` here is a no-op conversion -- `wide`'s pct_ columns are
  # already renamed to rate_ -- so the only thing re-running write_standard()
  # does for these two is drop_empty_measures() on just the filtered subset,
  # trimming the other cohort's now-structurally-empty columns from each.
  write_standard(wide %>% filter(grade %in% KG_GRADES),
                 "California (kindergarten)",
                 "./standard/data_kindergarten.csv.gz", from = "rate")
  write_standard(wide %>% filter(grade %in% G7_GRADES),
                 "California (7th grade)",
                 "./standard/data_grade7.csv.gz", from = "rate")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
