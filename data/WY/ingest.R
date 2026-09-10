library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")
source("../../resources/pdf_table.R")

# =============================================================================
# WY - County immunization report cards, Wyoming Department of Health
#
# One card per county per calendar year (2018 to 2023), built from the Wyoming
# Immunization Registry (WyIR) and the Immunization Waiver Database. Not a
# school survey: the coverage figures describe resident children by age group
# (19-35 months; 6 or 7 years; adolescents 13-17 and 16-18), which is why the
# measures carry the dose and age in their names, following the Wisconsin
# registry measures (rate_mmr_2dose_6y). The 2022 and 2023 cards add a
# "Kindergarteners" block taken from the annual Immunization Status Report,
# which is the school survey; those rows go to standard/data_kindergarten.csv.gz.
#
# Each card prints four columns per measure: the county figure, the Wyoming
# figure, the US figure from the National Immunization Survey (dropped from
# 2021), and the county's rank among the 23 counties. Every card in a year
# prints the same Wyoming figure, which is checked here and used for the
# statewide row.
#
# The cards are published as PDF (and as JPG for 2018) on health.wyo.gov, which
# sits behind Cloudflare bot management and refuses scripted requests
# intermittently, so nothing is downloaded here. raw/ holds one workbook per
# card, produced from the PDFs by PDF-to-Excel conversion or typed by hand,
# named "<year> <County>.xlsx", "<County>_<year>.xlsx" or "<County> <year>.xlsx"
# ("(1)"-style download suffixes are ignored). The one PDF in raw/, "2018
# Converse (1).pdf", is a browser print of the JPG card with no text layer, so
# Converse 2018 is not in the output.
#
# The conversions are not uniform. Depending on the file, a measure's values
# sit in the cells to the right of its label, or in the label's own cell as
# "4 DTaP   64%   64%   70.5%15" (county, state, US, rank run together), or
# two measures share one cell. The 2021 conversions print the waiver table as
# one text block with the county and state counts run together ("Religious
# 15971" is 15 and 971); those are split with the state count known from the
# hand-typed 2021 cards. The 2020 conversions lost four of the 19-35 month
# rows (4 DTaP, 1 MMR, 3 HepB, 4 PCV) in every file that was not typed by
# hand, so those are missing for 19 counties in 2020. The parser therefore
# reads each sheet as a bag of labelled lines rather than by position, and the
# checks below (header order, dose in the label, state figures agreeing across
# cards, waiver totals balancing) are what make that safe.
#
# Values are proportions in the workbooks (0.65, or "65%" where typed), so
# write_standard() is called with from = "rate".
#
# Time: the card year is a calendar year (the 2018 cards are named "CY-2018"
# on the source page; footnotes say "waivers approved in 2020"). It is dated
# to the school year that starts in that calendar year: 2020 -> 2020-09-01.
# Wisconsin dates its registry calendar year N to the school year ending in N;
# that difference is noted in README.md.
# =============================================================================

sources <- read_sources()
src <- source_entry(sources, "wdh_county_report_cards")

process <- dcf::dcf_process_record()

# ---- No download -------------------------------------------------------------
# The cards are added to raw/ by hand (see the header). The source page is
# recorded in sources.json for the weekly check; it is not scraped here.

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # ---- Helpers ---------------------------------------------------------------

  # Year and county from the file name.
  card_meta <- function(path) {
    fn <- tools::file_path_sans_ext(basename(path))
    fn <- trimws(sub("\\s*\\(\\d+\\)\\s*$", "", fn))
    m <- str_match(fn, "^(\\d{4})\\s+(.+)$")
    if (!is.na(m[1, 1])) return(list(year = as.integer(m[1, 2]), county = trimws(m[1, 3])))
    m <- str_match(fn, "^(.+?)[ _](\\d{4})$")
    if (!is.na(m[1, 1])) return(list(year = as.integer(m[1, 3]), county = trimws(m[1, 2])))
    stop("WY: cannot read county and year from file name ", basename(path), call. = FALSE)
  }

  # Lower case, ASCII, no whitespace: the form every label test below uses.
  # OCR renders a leading "1" as "l" or "I" ("lMMR", "I  Tdap") and the >=
  # sign as "::>", "::,", "2:" or "?." (the waiver labels allow for those).
  # Unicode is folded before anything else because Rscript may run in a C
  # locale (the escapes keep this file ASCII, as in resources/pdf_table.R).
  squash <- function(x) {
    x <- enc2utf8(as.character(x))
    x <- gsub("\u2265", ">=", x, fixed = TRUE)          # >= sign
    for (d in c("\u2013", "\u2014", "\u2212")) {      # en dash, em dash, minus
      x <- gsub(d, "-", x, fixed = TRUE)
    }
    x <- iconv(x, "UTF-8", "ASCII", sub = "?")
    x <- tolower(x)
    x <- gsub("::>", ">=", x, fixed = TRUE)
    x <- gsub("\\s+", "", x, perl = TRUE)
    sub("^[il](?=mmr|tdap|menacwy|var|hib|hep|pcv|polio|dtap)", "1", x, perl = TRUE)
  }

  # Every non-empty cell of a sheet as (row, col, text), in reading order.
  sheet_cells <- function(path, sheet) {
    d <- suppressMessages(readxl::read_excel(path, sheet = sheet, col_names = FALSE,
                                             col_types = "text"))
    empty <- data.frame(row = integer(), col = integer(), text = character(),
                        stringsAsFactors = FALSE)
    if (!nrow(d) || !ncol(d)) return(empty)
    m <- as.matrix(d)
    idx <- which(!is.na(m) & nzchar(trimws(m)), arr.ind = TRUE)
    if (!nrow(idx)) return(empty)
    out <- data.frame(row = idx[, 1], col = idx[, 2], text = trimws(m[idx]),
                      stringsAsFactors = FALSE)
    out[order(out$row, out$col), ]
  }

  # A cell that holds a number or a not-available marker, as printed.
  VALUE_TOKEN <- "^(\\d+(?:\\.\\d+)?%?|\\.\\d+|n/?a|nia)$"
  is_value_cell <- function(x) grepl(VALUE_TOKEN, tolower(x), perl = TRUE)

  # A printed value to a proportion. Percent strings are rescaled; a plain
  # number is a proportion. One conversion (2019 Sweetwater, 3 Polio) dropped
  # the percent sign and left "87" beside proportions; a whole number from 2
  # to 100 is read as percent points and reported, and anything else above 1
  # stops the run. "N/A" (and its OCR form "NIA") is NA.
  as_proportion <- function(x, where, label = x) {
    x <- tolower(trimws(x))
    out <- rep(NA_real_, length(x))
    pct <- !is.na(x) & grepl("%$", x)
    out[pct] <- suppressWarnings(as.numeric(sub("%$", "", x[pct]))) / 100
    plain <- !is.na(x) & !pct & grepl("^\\.?\\d", x)
    out[plain] <- suppressWarnings(as.numeric(x[plain]))
    unsigned <- !is.na(out) & out > 1 & out <= 100 & out == round(out)
    if (any(unsigned)) {
      message("WY: ", where, ": percent printed without its sign, read as percent points: ",
              paste(unique(label[unsigned]), collapse = "; "))
      out[unsigned] <- out[unsigned] / 100
    }
    bad <- !is.na(out) & (out > 1 | out < 0)
    if (any(bad)) {
      stop("WY: ", where, ": value(s) not a proportion: ",
           paste(x[bad], collapse = ", "), call. = FALSE)
    }
    out
  }

  # ---- Header order check ----------------------------------------------------
  # Every layout prints the county column before the Wyoming/state column,
  # either as separate header cells or as one "County  Wyoming  County" line.
  # The parser takes the first value as the county and the second as the
  # state, so a card must show that order somewhere and the reverse nowhere.
  COUNTY_HEADERS <- c("county", "county(wyir)", "county(wylr)", "countyrate")
  STATE_HEADERS <- c("state", "wyoming", "wyoming(wyir)", "wyoming(wylr)", "state(wyir)",
                     "wyomingrate")
  header_order <- function(cells) {
    lines <- squash(unlist(strsplit(cells$text, "\n", fixed = TRUE)))
    in_line <- grepl("^county(\\(wy[il]r\\))?(wyoming|state)", lines, perl = TRUE)
    rev_line <- grepl("^(wyoming|state)(\\(wy[il]r\\))?county", lines, perl = TRUE)
    ok_row <- FALSE
    rev_row <- FALSE
    for (r in split(cells, cells$row)) {
      s <- squash(r$text)
      c_col <- r$col[s %in% COUNTY_HEADERS]
      s_col <- r$col[s %in% STATE_HEADERS]
      if (length(c_col) && length(s_col)) {
        if (min(c_col) < min(s_col)) ok_row <- TRUE else rev_row <- TRUE
      }
    }
    if (any(rev_line) || rev_row) return("reversed")
    if (any(in_line) || ok_row) return("county_first")
    "none"
  }

  # ---- Coverage measures -----------------------------------------------------
  # Section headings set the cohort for the labels that follow. The hand-typed
  # workbooks carry the section in the first column of every row and the
  # conversions print it once as a heading, so cells are scanned left to
  # right and top to bottom with the section carried forward.
  # OCR mangles the word "Children" on some cards ("C(Whildren"), so the age
  # band is what identifies the two child sections.
  section_of <- function(s) {
    dplyr::case_when(
      grepl("19-?35months", s) ~ "19_35m",
      grepl("^c.{0,4}hildren[^a-z0-9]{0,4}[67]-?years", s, perl = TRUE) ~ "6y",
      grepl("^kindergarten", s) ~ "k",
      grepl("^adolescents", s) ~ "adolescent",
      grepl("^adults", s) ~ "adult",
      TRUE ~ NA_character_
    )
  }

  # Antigen and dose from a squashed label; NA antigen for anything that is
  # not a measure label. The MenACWY dose is read from the age band when the
  # label has no leading count ("Men ACWY (13-17)").
  LABEL_PATTERN <- paste0(
    "^([0-9])?(4:3:1:3:3:1:4|5:4:2:3:3:2:4|dtap|polio|mmr|hib|hepb|var(?:icella)?|pcv|",
    "hepa|rotavirus|menacwy\\(?(1[36])?|hpv|tdap|ppsv23|zoster)(?![a-z])")
  label_antigen <- function(s) {
    m <- str_match(s, LABEL_PATTERN)
    key <- m[, 3]
    antigen <- dplyr::case_when(
      is.na(key) ~ NA_character_,
      key %in% c("4:3:1:3:3:1:4", "5:4:2:3:3:2:4") ~ "series",
      key %in% c("var", "varicella") ~ "varicella",
      grepl("^menacwy", key) ~ "menacwy",
      TRUE ~ key
    )
    dose <- suppressWarnings(as.integer(m[, 2]))
    men_age <- m[, 4]
    dose <- ifelse(!is.na(antigen) & antigen == "menacwy" & is.na(dose) & !is.na(men_age),
                   ifelse(men_age == "13", 1L, 2L), dose)
    list(antigen = antigen, dose = dose)
  }

  # Section + antigen -> measure, with the dose the card prints for it. The
  # kindergarten block prints a different dose count each year and is not
  # checked.
  MEASURE_MAP <- tibble::tribble(
    ~section, ~antigen, ~dose, ~measure,
    "19_35m", "series", NA, "pct_series_19_35m",
    "19_35m", "dtap", 4, "pct_dtap_4dose_19_35m",
    "19_35m", "polio", 3, "pct_polio_3dose_19_35m",
    "19_35m", "mmr", 1, "pct_mmr_1dose_19_35m",
    "19_35m", "hib", 3, "pct_hib_3dose_19_35m",
    "19_35m", "hepb", 3, "pct_hep_b_3dose_19_35m",
    "19_35m", "varicella", 1, "pct_varicella_1dose_19_35m",
    "19_35m", "pcv", 4, "pct_pcv_4dose_19_35m",
    "19_35m", "hepa", 2, "pct_hep_a_2dose_19_35m",
    "19_35m", "rotavirus", 2, "pct_rotavirus_2dose_19_35m",
    "6y", "series", NA, "pct_series_6y",
    "6y", "dtap", 5, "pct_dtap_5dose_6y",
    "6y", "mmr", 2, "pct_mmr_2dose_6y",
    "6y", "varicella", 2, "pct_varicella_2dose_6y",
    "adolescent", "menacwy", 1, "pct_menacwy_1dose_13_17y",
    "adolescent", "menacwy", 2, "pct_menacwy_2dose_16_18y",
    "adolescent", "hpv", 2, "pct_hpv_2dose_13_17y",
    "adolescent", "tdap", 1, "pct_tdap_13_17y",
    "k", "dtap", NA, "pct_dtap",
    "k", "mmr", NA, "pct_mmr",
    "k", "varicella", NA, "pct_varicella",
    "k", "polio", NA, "pct_polio"
  )

  # Coverage rows of one sheet: section, antigen, dose and the county and
  # state values as printed.
  parse_coverage <- function(cells, where) {
    section <- NA_character_
    out <- list()
    for (r in split(cells, cells$row)) {
      for (i in seq_len(nrow(r))) {
        lines <- strsplit(r$text[i], "\n", fixed = TRUE)[[1]]
        lines <- lines[nzchar(trimws(lines))]
        for (ln in lines) {
          s <- squash(ln)
          sec <- section_of(s)
          if (!is.na(sec)) { section <- sec; next }
          lab <- label_antigen(s)
          if (is.na(lab$antigen)) next
          if (lab$antigen %in% c("ppsv23", "zoster")) next
          # the 2018 cards print a "Varicella" case count under
          # Vaccine-Preventable Diseases; a coverage label carries a dose or
          # the "VAR" spelling
          if (s == "varicella") next
          sec_here <- section
          if (lab$antigen %in% c("menacwy", "hpv", "tdap")) sec_here <- "adolescent"
          # a series label opens its block, so it also resets the section for
          # the cards whose heading OCR did not read
          if (lab$antigen == "series") {
            sec_here <- if (grepl("^5:4:2", s)) "6y" else "19_35m"
            section <- sec_here
          }
          if (is.na(sec_here)) {
            stop("WY: ", where, ": measure label '", ln, "' before any section heading",
                 call. = FALSE)
          }
          if (sec_here == "adult") next
          # values embedded in the line ("4 DTaP   64%   64%   70.5%15"), else
          # the cells to the right up to the first that is not a value
          embedded <- str_extract_all(ln, "\\d+(?:\\.\\d+)?%")[[1]]
          if (length(embedded) >= 2L) {
            vals <- embedded[1:2]
          } else {
            right <- r$text[seq_len(nrow(r)) > i]
            right <- right[!grepl("\n", right, fixed = TRUE)]
            stop_at <- which(!is_value_cell(right))
            if (length(stop_at)) right <- right[seq_len(stop_at[1] - 1L)]
            vals <- c(right, NA, NA)[1:2]
          }
          out[[length(out) + 1L]] <- data.frame(
            section = sec_here, antigen = lab$antigen, dose = lab$dose, label = ln,
            county_raw = vals[1], state_raw = vals[2], stringsAsFactors = FALSE)
        }
      }
    }
    if (!length(out)) return(NULL)
    bind_rows(out)
  }

  # ---- Immunization waivers --------------------------------------------------
  # Four counts, always in the order under 5, 5 and over, religious, medical,
  # each with a county and a state figure. Located from the "Immunization
  # Waivers" heading (never from the footnote that also names the database).
  # In the 2021 conversions the whole table is one text block in which each
  # line ends with the county and state counts run together ("Religious
  # 15971"); those come back with glued = TRUE and no state figure and are
  # split later against the state figure the other cards print. A row with
  # one figure where two are expected (OCR dropped one) comes back with
  # single = TRUE and the figure in state_raw; which figure it is is decided
  # later, again against the state figure the other cards print.
  waiver_kind <- function(s) {
    dplyr::case_when(
      grepl("religious", s) ~ "religious",
      grepl("medical", s) ~ "medical",
      grepl("<5", s) | grepl("^childrenunder", s) ~ "under_5",
      s == "children" ~ "children",
      # the only other "Children ..." row is 5 and over, however OCR spelt it
      grepl("^children", s) | grepl(">=5|^5years", s) ~ "5_plus",
      TRUE ~ NA_character_
    )
  }
  GLUED_LINE <- "^(.*?\\S)\\s+(\\d+)\\s*$"

  parse_waivers <- function(cells, where, whole_sheet = FALSE) {
    if (whole_sheet) {
      block <- cells
    } else {
      first_line <- squash(sub("\n.*$", "", cells$text))
      heading <- cells[grepl("^immunizationwaivers?[^a-z]*$", first_line, perl = TRUE), ]
      if (!nrow(heading)) return(NULL)
      block <- cells[cells$row >= min(heading$row), ]
    }
    found <- list()
    add <- function(kind, county, state, glued, single = FALSE) {
      found[[length(found) + 1L]] <<- list(kind = kind, county = county, state = state,
                                            glued = glued, single = single)
    }
    for (r in split(block, block$row)) {
      if (length(found) >= 4L) break
      for (i in seq_len(nrow(r))) {
        if (length(found) >= 4L) break
        text <- r$text[i]
        lines <- strsplit(text, "\n", fixed = TRUE)[[1]]
        lines <- lines[nzchar(trimws(lines))]
        glued <- str_match(lines, GLUED_LINE)
        if (length(lines) > 1L && any(!is.na(glued[, 1]))) {
          # the one-block form
          for (j in seq_along(lines)) {
            if (length(found) >= 4L) break
            if (is.na(glued[j, 1])) next
            kind <- waiver_kind(squash(glued[j, 2]))
            if (is.na(kind)) next
            add(kind, glued[j, 3], NA_character_, TRUE)
          }
          next
        }
        if (nchar(text) > 60L) next
        kind <- waiver_kind(squash(text))
        if (is.na(kind)) next
        # OCR reads a lone 1 as "I" or "l" and 0 as "O" in the count cells
        right <- chartr("IlO", "110", r$text[seq_len(nrow(r)) > i])
        stop_at <- which(!grepl("^\\d+$", right))
        if (length(stop_at)) right <- right[seq_len(stop_at[1] - 1L)]
        if (!length(right)) next
        if (length(right) == 1L) add(kind, NA_character_, right[1], FALSE, single = TRUE)
        else add(kind, right[1], right[2], FALSE)
      }
    }
    if (length(found) != 4L) {
      stop("WY: ", where, ": expected 4 waiver rows, found ", length(found), call. = FALSE)
    }
    kinds <- vapply(found, function(f) f$kind, character(1))
    expected <- c("under_5", "5_plus", "religious", "medical")
    labelled <- kinds != "children"
    if (any(kinds[labelled] != expected[labelled])) {
      stop("WY: ", where, ": waiver rows out of order: ", paste(kinds, collapse = ", "),
           call. = FALSE)
    }
    data.frame(kind = expected,
               county_raw = vapply(found, function(f) f$county, character(1)),
               state_raw = vapply(found, function(f) f$state, character(1)),
               glued = vapply(found, function(f) f$glued, logical(1)),
               single = vapply(found, function(f) f$single, logical(1)),
               stringsAsFactors = FALSE)
  }

  # ---- Read every card -------------------------------------------------------
  files <- list.files("raw", pattern = "\\.xlsx$", full.names = TRUE)
  files <- files[!grepl("^~\\$", basename(files))]
  if (!length(files)) stop("WY: no workbooks in raw/ to process.", call. = FALSE)
  # A PDF in raw/ is only ever the image-only 2018 Converse card. A PDF with
  # a text layer would need a parser of its own, so it stops the run.
  for (p in list.files("raw", pattern = "\\.pdf$", full.names = TRUE)) {
    meta <- card_meta(p)
    txt <- paste(pdf_text_pages(p), collapse = " ")
    if (grepl("4:3:1:3:3:1:4|Immunization Waivers", txt)) {
      stop("WY: ", basename(p), " is a text PDF, which this ingest does not parse", call. = FALSE)
    }
    message(sprintf("WY: %s has no text layer; %s %d is not parsed",
                    basename(p), meta$county, meta$year))
  }

  cards <- lapply(files, function(path) {
    meta <- card_meta(path)
    where <- basename(path)
    sheets <- readxl::excel_sheets(path)
    cells_by_sheet <- lapply(sheets, function(s) sheet_cells(path, s))
    ord <- vapply(Filter(nrow, cells_by_sheet), header_order, character(1))
    if (any(ord == "reversed")) {
      stop("WY: ", where, ": state column before county column", call. = FALSE)
    }
    if (!any(ord == "county_first")) {
      stop("WY: ", where, ": no County / Wyoming header found", call. = FALSE)
    }

    cov <- bind_rows(lapply(seq_along(sheets), function(i) {
      cc <- cells_by_sheet[[i]]
      if (!nrow(cc)) return(NULL)
      parse_coverage(cc, paste0(where, " / ", sheets[i]))
    }))
    # The waiver table is read from the first sheet that carries one (Carbon
    # 2020 has a second, scrambled copy on its own sheet). The hand-typed
    # workbooks keep it on a "Waivers" sheet with no heading.
    wv <- NULL
    for (i in seq_along(sheets)) {
      cc <- cells_by_sheet[[i]]
      if (!nrow(cc)) next
      wv <- parse_waivers(cc, paste0(where, " / ", sheets[i]),
                          whole_sheet = grepl("^waivers?$", sheets[i], ignore.case = TRUE))
      if (!is.null(wv)) break
    }
    if (is.null(wv)) stop("WY: ", where, ": no Immunization Waivers table", call. = FALSE)
    list(file = where, year = meta$year, county = meta$county, coverage = cov, waivers = wv)
  })

  # ---- Coverage: labels to measures ------------------------------------------
  coverage <- bind_rows(lapply(cards, function(cd) {
    if (is.null(cd$coverage) || !nrow(cd$coverage)) return(NULL)
    cd$coverage %>% mutate(file = cd$file, year = cd$year, county = cd$county)
  }))
  no_dose <- coverage %>% filter(antigen == "menacwy", is.na(dose))
  if (nrow(no_dose)) {
    stop("WY: MenACWY label without a dose or age band: ",
         paste(unique(sprintf("'%s' in %s", no_dose$label, no_dose$file)), collapse = "; "),
         call. = FALSE)
  }
  coverage <- coverage %>%
    left_join(MEASURE_MAP, by = c("section", "antigen"), relationship = "many-to-many",
              suffix = c("", "_expected")) %>%
    filter(antigen != "menacwy" | dose == dose_expected)

  unknown <- coverage %>% filter(is.na(measure))
  if (nrow(unknown)) {
    stop("WY: label(s) with no measure: ",
         paste(unique(sprintf("'%s' [%s] in %s", unknown$label, unknown$section, unknown$file)),
               collapse = "; "), call. = FALSE)
  }
  wrong_dose <- coverage %>%
    filter(section != "k", !is.na(dose), !is.na(dose_expected), dose != dose_expected)
  if (nrow(wrong_dose)) {
    stop("WY: dose in label does not match the section: ",
         paste(unique(sprintf("'%s' under %s in %s", wrong_dose$label, wrong_dose$section,
                              wrong_dose$file)), collapse = "; "), call. = FALSE)
  }
  dup <- coverage %>% count(file, measure) %>% filter(n > 1)
  if (nrow(dup)) {
    stop("WY: measure read more than once: ",
         paste(sprintf("%s in %s", dup$measure, dup$file), collapse = "; "), call. = FALSE)
  }
  coverage <- coverage %>%
    mutate(county_value = as_proportion(county_raw, "county values",
                                        sprintf("%s '%s' %s", file, label, county_raw)),
           state_value = as_proportion(state_raw, "state values",
                                       sprintf("%s '%s' %s", file, label, state_raw)),
           k_dose = if_else(section == "k", dose, NA_integer_)) %>%
    select(file, year, county, section, measure, label, k_dose, county_value, state_value)

  # ---- Waivers: split glued counts, check the balance ------------------------
  waivers <- bind_rows(lapply(cards, function(cd) {
    cd$waivers %>% mutate(file = cd$file, year = cd$year, county = cd$county)
  }))
  state_counts <- waivers %>%
    filter(!glued, !single) %>%
    group_by(year, kind) %>%
    summarise(n_distinct = n_distinct(state_raw), state_raw = first(state_raw),
              .groups = "drop")
  if (any(state_counts$n_distinct > 1)) {
    bad <- state_counts %>% filter(n_distinct > 1)
    stop("WY: state waiver counts disagree across cards: ",
         paste(sprintf("%d %s", bad$year, bad$kind), collapse = ", "), call. = FALSE)
  }
  waivers <- waivers %>%
    left_join(state_counts %>% select(year, kind, state_known = state_raw),
              by = c("year", "kind")) %>%
    mutate(
      splittable = glued & !is.na(state_known) & str_ends(county_raw, state_known) &
        nchar(county_raw) > nchar(state_known),
      county_raw = if_else(splittable,
                           str_sub(county_raw, 1L, nchar(county_raw) - nchar(state_known)),
                           county_raw),
      state_raw = if_else(glued, state_known, state_raw)
    )
  # A lone figure is the state figure when it matches what the other cards
  # print (the county figure was lost), otherwise the county figure.
  lone <- waivers %>% filter(single)
  if (nrow(lone)) {
    message("WY: one waiver figure where two are expected in: ",
            paste(sprintf("%s %s=%s", lone$file, lone$kind, lone$state_raw), collapse = "; "))
  }
  waivers <- waivers %>%
    mutate(
      county_raw = if_else(single & !is.na(state_known) & state_raw != state_known,
                           state_raw, county_raw),
      state_raw = if_else(single, state_known, state_raw)
    )
  unsplit <- waivers %>% filter(glued, !splittable)
  if (nrow(unsplit)) {
    warning("WY: waiver count(s) run together with the state figure could not be split: ",
            paste(sprintf("%s %s=%s", unsplit$file, unsplit$kind, unsplit$county_raw),
                  collapse = "; "), call. = FALSE)
    waivers$county_raw[waivers$glued & !waivers$splittable] <- NA_character_
  }
  waivers <- waivers %>%
    mutate(county_value = as.numeric(county_raw), state_value = as.numeric(state_raw)) %>%
    select(file, year, county, kind, county_value, state_value)

  # Under 5 + 5 and over = religious + medical on every card (the two are
  # splits of the same waivers). A county count the conversion dropped is
  # recovered from that identity when it is the only one missing on the
  # card, and a card that does not balance is reported.
  wide <- waivers %>%
    select(file, kind, county_value) %>%
    pivot_wider(names_from = kind, values_from = county_value)
  kinds <- c("under_5", "5_plus", "religious", "medical")
  missing <- is.na(as.matrix(wide[kinds]))
  one_missing <- which(rowSums(missing) == 1L)
  for (i in one_missing) {
    k <- kinds[missing[i, ]]
    v <- switch(k,
      under_5 = wide$religious[i] + wide$medical[i] - wide$`5_plus`[i],
      `5_plus` = wide$religious[i] + wide$medical[i] - wide$under_5[i],
      religious = wide$under_5[i] + wide$`5_plus`[i] - wide$medical[i],
      medical = wide$under_5[i] + wide$`5_plus`[i] - wide$religious[i])
    if (v < 0) next
    message(sprintf("WY: %s: %s county count recovered as %d from the other three", wide$file[i], k, v))
    wide[[k]][i] <- v
  }
  waivers <- waivers %>%
    select(-county_value) %>%
    left_join(wide %>% pivot_longer(all_of(kinds), names_to = "kind", values_to = "county_value"),
              by = c("file", "kind"))
  unbalanced <- wide %>%
    filter(!is.na(under_5 + `5_plus` + religious + medical),
           under_5 + `5_plus` != religious + medical)
  if (nrow(unbalanced)) {
    warning("WY: waiver counts do not balance (under 5 + 5 and over != religious + medical) in: ",
            paste(unbalanced$file, collapse = ", "), call. = FALSE)
  }

  # ---- Statewide figures -----------------------------------------------------
  # Every card in a year prints the same Wyoming figure. A card that differs
  # is a conversion or typing error on that card; the value the other cards
  # agree on is kept when at least two thirds of them carry it, and the
  # dissenting cards are listed. With no such majority the measure has no
  # statewide value that year.
  cov_state <- coverage %>%
    filter(!is.na(state_value)) %>%
    group_by(year, measure) %>%
    summarise(
      n = n(),
      mode = { t <- table(state_value); as.numeric(names(t)[which.max(t)]) },
      n_mode = { t <- table(state_value); max(t) },
      dissent = paste(sprintf("%s=%s", file[state_value != mode], state_value[state_value != mode]),
                      collapse = ", "),
      .groups = "drop") %>%
    mutate(majority = n_mode >= 2 / 3 * n)
  disagree <- cov_state %>% filter(n_mode < n)
  if (nrow(disagree)) {
    message("WY: state figures differ between cards (the majority value is kept):\n  ",
            paste(sprintf("%d %s: %s on %d of %d cards; %s", disagree$year,
                          sub("^pct_", "", disagree$measure), disagree$mode, disagree$n_mode,
                          disagree$n, disagree$dissent), collapse = "\n  "))
  }
  no_state <- cov_state %>% filter(!majority)
  if (nrow(no_state)) {
    warning("WY: no statewide value for ",
            paste(sprintf("%s %d", sub("^pct_", "", no_state$measure), no_state$year),
                  collapse = ", "),
            ": fewer than two thirds of the cards agree", call. = FALSE)
  }

  # ---- Assemble --------------------------------------------------------------
  registry_measures <- MEASURE_MAP$measure[MEASURE_MAP$section != "k"]
  k_measures <- MEASURE_MAP$measure[MEASURE_MAP$section == "k"]
  waiver_measures <- c(under_5 = "N_waivers_under_5y", `5_plus` = "N_waivers_5y_plus",
                       religious = "N_religious_exempt", medical = "N_medical_exempt")

  county_rows <- full_join(
    coverage %>%
      select(year, county, measure, county_value) %>%
      pivot_wider(names_from = measure, values_from = county_value),
    waivers %>%
      mutate(measure = unname(waiver_measures[kind])) %>%
      select(year, county, measure, county_value) %>%
      pivot_wider(names_from = measure, values_from = county_value),
    by = c("year", "county")) %>%
    mutate(type = "county")

  state_rows <- full_join(
    cov_state %>%
      filter(majority) %>%
      select(year, measure, mode) %>%
      pivot_wider(names_from = measure, values_from = mode),
    state_counts %>%
      mutate(measure = unname(waiver_measures[kind]), state_value = as.numeric(state_raw)) %>%
      select(year, measure, state_value) %>%
      pivot_wider(names_from = measure, values_from = state_value),
    by = "year") %>%
    mutate(county = "Wyoming", type = "state")

  all_rows <- bind_rows(county_rows, state_rows) %>%
    join_county_fips("WY", statewide = "Wyoming") %>%
    mutate(time = school_year_time(year)) %>%
    arrange(time, desc(type), geography)

  registry_out <- all_rows %>%
    select(time, geography, geography_name, type,
           any_of(registry_measures), any_of(unname(waiver_measures)))
  k_years <- unique(coverage$year[coverage$section == "k"])
  k_out <- all_rows %>%
    filter(year %in% k_years) %>%
    mutate(grade = "Kindergarten") %>%
    select(time, geography, geography_name, type, grade, any_of(k_measures))

  # The kindergarten block prints a different dose count each year (5 DTaP,
  # 2 MMR, 2 varicella, 4 polio in 2022; 4, 1, 1, 3 in 2023).
  k_doses <- coverage %>%
    filter(section == "k") %>%
    distinct(year, measure, k_dose) %>%
    arrange(year, measure)
  if (nrow(k_doses)) {
    message("WY: kindergarten doses by year: ",
            paste(sprintf("%d %s=%s", k_doses$year, sub("^pct_", "", k_doses$measure),
                          k_doses$k_dose), collapse = ", "))
  }

  n_by_year <- registry_out %>% count(time, type)
  message("WY: ", nrow(registry_out), " rows: ",
          paste(sprintf("%s %s=%d", substr(n_by_year$time, 1, 4), n_by_year$type, n_by_year$n),
                collapse = ", "))
  present <- coverage %>%
    count(year, measure) %>%
    pivot_wider(names_from = year, values_from = n) %>%
    mutate(measure = sub("^pct_", "", measure))
  message("WY: cards carrying each measure, by year:\n",
          paste(capture.output(print(as.data.frame(present), row.names = FALSE)),
                collapse = "\n"))

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(registry_out, "Wyoming", "./standard/data.csv.gz", from = "rate")
  write_standard(k_out, "Wyoming kindergarten", "./standard/data_kindergarten.csv.gz",
                 from = "rate")
  update_latest_year(latest_school_year(out))

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
