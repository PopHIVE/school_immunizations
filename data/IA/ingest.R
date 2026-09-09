library(dcf)
library(dplyr)
library(stringr)
library(readr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")
source("../../resources/pdf_table.R")

# =============================================================================
# IA - School immunization audit by county
# Source: Iowa HHS (formerly IDPH) Immunization Program, annual school audit.
#
# Two series, both from the same audit:
#
#   * Kindergarten summary report by county (grade = "Kindergarten"). One PDF
#     per school year on publications.iowa.gov, "Kindergarten School Summary
#     <year>" (2025-26 is titled "Kindergarten Immunization Summary"; 2012-13
#     is just "kindergarten 2012-2013"). Each is a three- or four-page table
#     with one row per county and a State Total (Grand Total in 2012-13):
#     Certificate of Immunization, Provisional Certificates, Certificate of
#     Medical Exemption, Certificate of Religious Exemption, Invalid or No
#     Certificate, Total Valid Certificates, Total Enrollment, Percent Valid
#     Certificates. Total Valid is the sum of the four certificate kinds and
#     Total Enrollment is Total Valid plus Invalid; both identities are
#     asserted, as is the state row against the county sum. Every rate is
#     computed from the counts over Total Enrollment; the printed percent is
#     only checked against Total Valid / Total Enrollment (see below).
#     Years posted: 2012-13, 2018-19, 2020-21, 2021-22, 2023-24, 2024-25,
#     2025-26. No 2013-14 to 2017-18, 2019-20 or 2022-23 report is on the
#     site (checked 2026-09-08 with several search queries).
#
#     The files are found through the site's search page. Results come 20 to
#     a page, so the ingest walks search_offset = 0, 20, 40, ... until a page
#     yields no matching link. The PDFs never change once posted, so a copy
#     already in raw/ that opens as a kindergarten summary is not
#     re-requested.
#
#   * K-12 medical and religious exemption certificates by county (grade =
#     "K-12"), 2011-12 to 2024-25, from the audit CSVs exported by hand into
#     raw/K-12/ (one file per year and exemption type, UTF-16 tab-delimited).
#     Rates are computed from the counts; see the note in read_exempt().
#
# County rows have type = "county"; the kindergarten state row type = "state"
# with geography "19". Every rate is a proportion computed here, so
# write_standard() is called with from = "rate".
# =============================================================================

sources <- read_sources()
k_src <- source_entry(sources, "kindergarten_summary_pdf")

dir.create("raw", showWarnings = FALSE)
process <- dcf::dcf_process_record()
prev <- process$fetch_state

# ---- Kindergarten PDFs: discovery and download -------------------------------

# raw/kindergarten_county_<YYYY>-<YY>.pdf from the posted name, which carries
# the school year as "2024-25", "2023-24 ..." or "2012-2013". Both years are
# kept in the name so scripts/check_sources.R can match the posted file to
# the stored one.
k_dest <- function(u) {
  f <- utils::URLdecode(basename(u))
  m <- str_match(f, "(20\\d{2})-(?:20)?(\\d{2})")
  start <- suppressWarnings(as.integer(m[1, 2]))
  end2 <- suppressWarnings(as.integer(m[1, 3]))
  if (is.na(start) || is.na(end2) || end2 != (start + 1L) %% 100L) {
    stop("IA: cannot read a school year from kindergarten summary name '", f, "'",
         call. = FALSE)
  }
  file.path("raw", sprintf("kindergarten_county_%d-%02d.pdf", start, end2))
}

# A PDF that is not the kindergarten summary (a search page served as a
# file, a different report under the same name) is rejected before it is
# stored.
check_k_pdf <- function(path) {
  txt <- pdftools::pdf_text(path)
  if (!any(grepl("kindergarten", txt, ignore.case = TRUE) &
           grepl("summary report", txt, ignore.case = TRUE))) {
    stop("not a kindergarten summary report")
  }
}

# Walk the search results 20 at a time. A page that cannot be fetched is a
# warning from discover_links(); the walk stops there and the parse proceeds
# on the committed raw/ files.
search_links <- function(page_url, pattern, step = 20L, max_pages = 5L) {
  found <- list()
  for (offset in seq(0L, by = step, length.out = max_pages)) {
    pg <- if (offset == 0L) page_url else paste0(page_url, "&search_offset=", offset)
    l <- discover_links(pg, pattern, must_find = FALSE)
    if (!isTRUE(attr(l, "fetched")) || !nrow(l)) break
    found[[length(found) + 1L]] <- l
  }
  if (!length(found)) return(character())
  unique(do.call(rbind, found)$url)
}

k_urls <- search_links(k_src$page_url, k_src$pattern)
if (length(k_urls)) {
  recs <- fetch_many(
    k_urls, dest_fn = k_dest, type = "pdf", validate = check_k_pdf,
    if_exists = "skip", previous = prev
  )
  fetch_summary(recs, "IA kindergarten summaries")
  process <- record_fetch(process, recs)
} else {
  warning("IA: no kindergarten summary PDFs discovered; parsing the committed raw/ copies",
          call. = FALSE)
}

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # ---- Kindergarten summary PDFs --------------------------------------------
  K_COLS <- c("n_immunization_certificate", "n_provisional", "n_medical_exempt",
              "n_religious_exempt", "n_no_certificate", "n_valid_certificate",
              "n_enrolled", "pct_valid_published")
  K_COUNTS <- K_COLS[1:7]

  # The printed percent is checked against Total Valid / Total Enrollment to
  # one unit of its own last printed digit (two decimals in 2012-13, one
  # otherwise, none for whole-number cells such as "100"). Half a unit would
  # be the natural rounding tolerance, but the reports round twice -- to two
  # decimals and then to one -- so a ratio of 98.4456 prints as 98.5 (two or
  # four counties a year), and 2025-26 prints a bare "100%" for four counties
  # whose counts give 99.5-99.6%. A column misread is off by whole points and
  # still stops the run.
  check_published_percent <- function(published, valid, enrolled, labels, fn) {
    txt <- trimws(sub("%$", "", pdf_ascii(published)))
    decimals <- ifelse(grepl("\\.", txt), nchar(sub("^[^.]*\\.", "", txt)), 0L)
    tol <- 10^(-decimals) / 100 + 1e-9
    computed <- rate_from_counts(valid, enrolled)
    off <- abs(pdf_number(published) / 100 - computed) > tol
    if (any(off)) {
      i <- which(off)[1]
      stop(sprintf(
        "IA %s: published percent disagrees with Total Valid / Total Enrollment beyond rounding on %d row(s); first: %s prints %s but %d / %d = %.4f",
        fn, sum(off), labels[i], published[i], valid[i], enrolled[i], computed[i]),
        call. = FALSE)
    }
  }

  read_kindergarten <- function(path) {
    fn <- basename(path)
    start <- as.integer(str_match(fn, "(20\\d{2})-\\d{2}\\.pdf$")[1, 2])
    txt <- pdf_text_pages(path)

    # The school year in the report title must agree with the file name.
    title <- head(strsplit(txt[[1]], "\n", fixed = TRUE)[[1]], 6)
    title_end <- school_year_end_from_label(title)
    title_end <- title_end[!is.na(title_end)]
    if (length(title_end) != 1L || title_end != start + 1L) {
      stop("IA ", fn, ": report title says school year ending ",
           paste(title_end, collapse = "/"), ", file name says ", start + 1L,
           call. = FALSE)
    }

    rows <- pdf_table_rows(path, label = "^[A-Z][A-Za-z .'-]+$", n_fields = 8,
                           col_names = K_COLS, text = txt)
    pdf_expect_rows(rows, 100L, paste("IA", fn, "(99 counties + state total)"))

    d <- rows["label"]
    for (col in K_COUNTS) d[[col]] <- pdf_number(rows[[col]])
    if (anyNA(d[K_COUNTS])) {
      stop("IA ", fn, ": a count cell did not parse as a number", call. = FALSE)
    }

    # Column-order guards: the identities the table is built on.
    with(d, {
      if (!all(n_valid_certificate == n_immunization_certificate + n_provisional +
               n_medical_exempt + n_religious_exempt)) {
        stop("IA ", fn, ": Total Valid Certificates is not the sum of the four certificate columns",
             call. = FALSE)
      }
      if (!all(n_enrolled == n_valid_certificate + n_no_certificate)) {
        stop("IA ", fn, ": Total Enrollment is not Total Valid plus Invalid or No Certificate",
             call. = FALSE)
      }
    })
    is_total <- grepl("^(State|Grand) Total$", d$label)
    if (sum(is_total) != 1L) {
      stop("IA ", fn, ": expected one State Total row, found ", sum(is_total), call. = FALSE)
    }
    county_sum <- colSums(d[!is_total, K_COUNTS])
    if (!all(county_sum == unlist(d[is_total, K_COUNTS]))) {
      stop("IA ", fn, ": the State Total row is not the sum of the county rows", call. = FALSE)
    }
    check_published_percent(rows$pct_valid_published, d$n_valid_certificate,
                            d$n_enrolled, d$label, fn)

    d %>%
      transmute(
        county = label,
        time = as.Date(school_year_time(start)),
        grade = "Kindergarten",
        n_immunization_certificate, n_provisional, n_medical_exempt,
        n_religious_exempt, n_no_certificate, n_valid_certificate,
        total_enrolled = n_enrolled,
        pct_immunization_certificate = rate_from_counts(n_immunization_certificate, n_enrolled),
        pct_provisional = rate_from_counts(n_provisional, n_enrolled),
        pct_medical_exempt = rate_from_counts(n_medical_exempt, n_enrolled),
        pct_religious_exempt = rate_from_counts(n_religious_exempt, n_enrolled),
        pct_no_certificate = rate_from_counts(n_no_certificate, n_enrolled),
        pct_valid_certificate = rate_from_counts(n_valid_certificate, n_enrolled)
      )
  }

  k_files <- list.files("raw", pattern = "^kindergarten_county_\\d{4}-\\d{2}\\.pdf$",
                        full.names = TRUE)
  if (!length(k_files)) {
    stop("IA: no kindergarten summary PDFs in raw/ to process.", call. = FALSE)
  }
  data_k <- bind_rows(lapply(k_files, read_kindergarten)) %>%
    join_county_fips("IA", statewide = c("State Total", "Grand Total")) %>%
    mutate(
      type = if_else(nchar(geography) == 2L, "state", "county"),
      geography_name = if_else(type == "state", "Iowa", geography_name)
    )

  # ---- K-12 exemption CSVs --------------------------------------------------
  parse_end_year <- function(path) {
    m <- str_match(basename(path), "(20\\d{2})-(\\d{2})")
    if (is.na(m[1, 1])) return(NA_integer_)
    as.integer(paste0(substr(m[1, 2], 1, 2), m[1, 3]))
  }

  read_exempt <- function(path, type) {
    end_year <- parse_end_year(path)
    time <- as.Date(school_year_time_from_end(end_year))

    d <- read_delim(
      path,
      delim = "\t",
      locale = locale(encoding = "UTF-16LE"),
      show_col_types = FALSE
    )

    d %>%
      transmute(
        county = str_to_title(str_trim(County)),
        time = time,
        total_enrolled = readr::parse_number(as.character(`Total Enrolled`)),
        n_exempt = readr::parse_number(as.character(`Number of Certificates`)),
        # Computed from the counts the file already carries, not parsed from
        # `Percent of Certificates`.
        #
        # HHS medical-exemption rates are all below 1 percent, so the old
        # column-global `if (max(y) <= 1) y * 100` test fired on those files and
        # multiplied every value by 100: Dickinson County 2020-21 has 27 medical
        # certificates against 2,707 enrolled -- a true 1.0% -- and shipped as
        # 100%. That is what pushed pct_full_exempt above 100.
        #
        # A rate computed from numerator and denominator has no scale to get
        # wrong, so this needs no assumption about how the file is formatted.
        pct_exempt = rate_from_counts(n_exempt, total_enrolled),
        exempt_type = type
      )
  }

  med_files <- list.files("raw/K-12/Medical Exemption", pattern = "\\.csv$", full.names = TRUE)
  rel_files <- list.files("raw/K-12/Religious Exemption", pattern = "\\.csv$", full.names = TRUE)
  med_files <- med_files[!str_detect(basename(med_files), "^~\\$")]
  rel_files <- rel_files[!str_detect(basename(rel_files), "^~\\$")]

  data_med <- bind_rows(lapply(med_files, read_exempt, type = "medical"))
  data_rel <- bind_rows(lapply(rel_files, read_exempt, type = "religious"))

  data_k12 <- full_join(
    data_med %>% select(-exempt_type) %>%
      rename(n_medical_exempt = n_exempt, pct_medical_exempt = pct_exempt),
    data_rel %>% select(-exempt_type) %>%
      rename(n_personal_exempt = n_exempt, pct_personal_exempt = pct_exempt),
    by = c("county", "time", "total_enrolled")
  ) %>%
    mutate(
      N_full_exempt = if_else(
        is.na(n_medical_exempt) & is.na(n_personal_exempt),
        NA_real_,
        coalesce(n_medical_exempt, 0) + coalesce(n_personal_exempt, 0)
      ),
      # Taken straight from the combined count over the same denominator, rather
      # than by adding the two component rates: the medical and religious files
      # are joined on total_enrolled, so a year where the two disagree on a
      # county's enrolment would otherwise sum rates with different bases.
      pct_full_exempt = rate_from_counts(N_full_exempt, total_enrolled),
      grade = "K-12"
    ) %>%
    # The source title-cases county names, which mangles O'Brien to "O'brien".
    join_county_fips("IA") %>%
    mutate(type = "county")

  # ---- Assemble ---------------------------------------------------------------
  data_out <- bind_rows(data_k, data_k12) %>%
    transmute(
      time, geography, geography_name, type, grade,
      N_immunization_certificate = n_immunization_certificate,
      N_provisional = n_provisional,
      N_medical_exempt = n_medical_exempt,
      N_religious_exempt = n_religious_exempt,
      N_personal_exempt = n_personal_exempt,
      N_full_exempt,
      N_no_certificate = n_no_certificate,
      N_valid_certificate = n_valid_certificate,
      total_enrolled,
      pct_immunization_certificate, pct_provisional, pct_medical_exempt,
      pct_religious_exempt, pct_personal_exempt, pct_full_exempt,
      pct_no_certificate, pct_valid_certificate
    ) %>%
    arrange(time, grade, factor(type, levels = c("state", "county")), geography)

  message(sprintf(
    "IA: %d kindergarten rows (%d state) for %d school years, %d K-12 rows for %d school years",
    sum(data_out$grade == "Kindergarten"),
    sum(data_out$grade == "Kindergarten" & data_out$type == "state"),
    n_distinct(data_out$time[data_out$grade == "Kindergarten"]),
    sum(data_out$grade == "K-12"),
    n_distinct(data_out$time[data_out$grade == "K-12"])))

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(data_out, "Iowa", "standard/data.csv.gz", from = "rate")
  update_latest_year(latest_school_year(out[out$grade == "Kindergarten", ]),
                     ids = "kindergarten_summary_pdf")
  update_latest_year(latest_school_year(out[out$grade == "K-12", ]),
                     ids = "audit_exemption_csvs")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
