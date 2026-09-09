library(dcf)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")
source("../../resources/fetch.R")
source("../../resources/pdf_table.R")

# =============================================================================
# SC - School immunization certification by school, and county exemptions
# Source: South Carolina Department of Public Health (DPH).
#
# Two sources:
#
#   45-day reports  https://dph.sc.gov/health-wellness/child-teen-health/school-vaccination-coverage-data
#     "45-Day Report of Schools with Required Immunization Certification",
#     one PDF per school year (2024-25 and 2025-26 so far), discovered from
#     the page. Each is a single school-level table: Region, County, Type
#     (Public/Private), School Name, City, Grade Range, Total Students, and
#     "% Students with Required Immunizations". The percent is a whole number
#     and is top-coded ">96%" for about 30% of schools; a handful of small
#     schools print "<5%", and Total Students is printed "<10" for the
#     smallest. The posted file name carries a revision date
#     (45_Day_Report_25-26_20260310.pdf) and DPH reissues both years, so the
#     raw copy is named by school year and re-fetched when the posted name
#     changes or when it is the latest year.
#
#     The table is set with single spaces between some columns and truncates
#     long cells at the column edge (region "Low Countr", county
#     "Spartanbur", and "ChesterfieldPrivate" where county and type touch), so
#     pdf_text() fields cannot be split on runs of spaces. The parse works
#     from pdf_data() word coordinates instead: the two right-hand columns are
#     read from the end of the line (a count then a percent), region, county
#     and type are read from the left by vocabulary, and the three free-text
#     columns between them (school name, city, grade range) are split at the
#     column start positions, found per file as the x positions where nearly
#     every row starts a word.
#
#     School rows only. The county figure is not computed from them: with a
#     third of the schools top-coded, an enrollment-weighted mean is not
#     defined.
#
#   county exemptions  raw/SC_2019-23.csv
#     Religious exemption counts and shares by county and statewide for
#     2018-19 to 2022-23, transcribed from the DPH five-year report. The DPH
#     county coverage page itself is Tableau Public with data access
#     disabled, so nothing can be fetched from it.
# =============================================================================

sources <- read_sources()
report_src <- source_entry(sources, "dph_45_day_reports")

process <- dcf::dcf_process_record()
prev <- process$fetch_state

# raw/45_day_report_<start year>.pdf from the posted name
# 45_Day_Report_25-26_20260310.pdf.
report_dest <- function(u) {
  m <- str_match(basename(u), "45_Day_Report_(\\d{2})-(\\d{2})_")
  y1 <- suppressWarnings(as.integer(m[, 2]))
  y2 <- suppressWarnings(as.integer(m[, 3]))
  if (is.na(y1) || is.na(y2) || y2 != y1 + 1L) {
    stop("SC: cannot read a school year from report name '", basename(u), "'", call. = FALSE)
  }
  file.path("raw", sprintf("45_day_report_%d.pdf", 2000L + y1))
}

check_report <- function(path) {
  first <- pdftools::pdf_text(path)[[1]]
  if (!grepl("45-Day Report", first, fixed = TRUE)) stop("first page is not a 45-day report")
}

# ---- Download ----------------------------------------------------------------
# The page being unreachable must not stop the run: raw/ is committed, so
# discovery warns and the parse proceeds on what is there. A report is
# re-fetched when the name DPH posts it under differs from the one recorded
# for that school year (a revision) and, for the latest year, on every run.
recs <- list()
links <- discover_links(report_src$page_url, report_src$pattern, must_find = FALSE)
if (nrow(links)) {
  dests <- vapply(links$url, report_dest, character(1), USE.NAMES = FALSE)
  latest <- dests[which.max(as.integer(str_extract(dests, "\\d{4}")))]
  for (i in seq_along(dests)) {
    p <- prev[[dests[i]]]
    revised <- is.null(p) || !identical(p$url, links$url[i]) || dests[i] == latest
    recs[[i]] <- fetch_file(
      links$url[i], dests[i], type = "pdf", validate = check_report,
      if_exists = if (revised) "replace" else "skip", previous = p
    )
    if (i < length(dests) && recs[[i]]$status %in% c("updated", "failed")) Sys.sleep(3)
  }
  fetch_summary(recs, "SC 45-day reports")
}
process <- record_fetch(process, recs)

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # ---- 45-day reports --------------------------------------------------------
  COUNT_PATTERN <- "^<?[0-9][0-9,]*$"
  PERCENT_PATTERN <- "^[<>]?[0-9]+%$"
  # Lines that are not data rows must be one of these; anything else stops
  # the run so a changed layout cannot drop rows in silence.
  HEADER_PATTERN <- "45-Day Report|^Region\\b|Students|^Grade\\b|^Total\\b|Required"
  REGION_PATTERN <- "^(upstate|midlands|low|lowcountry|country?|countr|pee|dee|peedee)$"
  TYPE_PATTERN <- "^(Public|Private|Charter|Parochial)$"

  sc_counties <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE,
                              progress = FALSE) %>%
    filter(state == "SC", nchar(geography) == 5) %>%
    pull(geography_name) %>%
    sub("\\s+County$", "", .)
  county_key <- county_fips_key(sc_counties)

  # The county as printed, or the full name where the cell was cut off at the
  # column edge ("Spartanbur", "Williamsbu"). A label that is not a county or
  # a unique prefix of one is returned as is, for join_county_fips() to
  # account for or reject.
  resolve_county <- function(x) {
    key <- county_fips_key(x)
    hit <- match(key, county_key)
    short <- is.na(hit) & nchar(key) >= 6
    for (i in which(short)) {
      cand <- which(startsWith(county_key, key[i]))
      if (length(cand) == 1L) hit[i] <- cand
    }
    ifelse(is.na(hit), x, sc_counties[hit])
  }

  # x positions where at least `min_count` words start (within a point), the
  # column starts of a left-aligned table.
  find_col_starts <- function(x, min_count) {
    tab <- table(round(x))
    v <- as.integer(names(tab))
    n <- as.integer(tab)
    win <- vapply(v, function(b) sum(n[abs(v - b) <= 1L]), integer(1))
    cand <- v[win >= min_count]
    cand <- cand[order(-win[match(cand, v)])]
    out <- integer()
    for (b in cand) if (!any(abs(out - b) <= 2L)) out <- c(out, b)
    sort(out)
  }

  # One report as a frame of school rows, all character.
  read_report <- function(path) {
    pages <- pdftools::pdf_data(path)
    first <- pdftools::pdf_text(path)[[1]]
    end_year <- school_year_end_from_label(str_extract(first, "20\\d{2}-20\\d{2} School Year"))
    if (is.na(end_year)) stop("SC: no school year in the title of ", basename(path), call. = FALSE)
    file_year <- as.integer(str_extract(basename(path), "\\d{4}"))
    if (!identical(end_year - 1L, file_year)) {
      stop(sprintf("SC: %s is titled %d-%d but is filed under %d", basename(path),
                   end_year - 1L, end_year, file_year), call. = FALSE)
    }

    rows <- list()
    for (p in seq_along(pages)) {
      w <- as.data.frame(pages[[p]], stringsAsFactors = FALSE)
      w$xend <- w$x + w$width
      for (ln in pdf_lines(w)) {
        txt <- ln$text
        n <- length(txt)
        is_data <- n >= 6L && grepl(PERCENT_PATTERN, txt[n]) && grepl(COUNT_PATTERN, txt[n - 1L])
        if (!is_data) {
          if (!grepl(HEADER_PATTERN, paste(txt, collapse = " "))) {
            stop(sprintf("SC: unrecognised line on page %d of %s: %s", p, basename(path),
                         paste(txt, collapse = " ")), call. = FALSE)
          }
          next
        }
        # region and county by vocabulary from the left; county and type can
        # be printed as one word where the county fills its cell
        k <- 1L
        while (k < n && grepl(REGION_PATTERN, gsub("[^a-z]", "", tolower(txt[k])))) k <- k + 1L
        if (k == 1L || k > 3L) {
          stop(sprintf("SC: no region on page %d of %s: %s", p, basename(path),
                       paste(txt, collapse = " ")), call. = FALSE)
        }
        region <- paste(txt[seq_len(k - 1L)], collapse = " ")
        m <- str_match(txt[k], "^(.*?)(Public|Private|Charter|Parochial)$")
        if (!is.na(m[1, 1]) && nchar(m[1, 2])) {
          county <- m[1, 2]
          type <- m[1, 3]
          k <- k + 1L
        } else {
          county <- txt[k]
          type <- txt[k + 1L]
          k <- k + 2L
        }
        if (!grepl(TYPE_PATTERN, type)) {
          stop(sprintf("SC: unrecognised school type '%s' on page %d of %s: %s", type, p,
                       basename(path), paste(txt, collapse = " ")), call. = FALSE)
        }
        mid <- ln[seq_len(n) >= k & seq_len(n) <= n - 2L, , drop = FALSE]
        rows[[length(rows) + 1L]] <- list(
          page = p, region = region, county = county, school_type = type,
          mid_text = mid$text, mid_x = mid$x, mid_xend = mid$xend,
          total = txt[n - 1L], pct = txt[n]
        )
      }
    }
    if (!length(rows)) stop("SC: no school rows in ", basename(path), call. = FALSE)

    # School name, city and grade range are left-aligned, so each column
    # start is an x where nearly every row starts a word. Exactly three are
    # expected; a fourth or a missing one means the layout changed.
    starts <- find_col_starts(unlist(lapply(rows, `[[`, "mid_x")), 0.5 * length(rows))
    if (length(starts) != 3L) {
      stop(sprintf("SC: expected 3 text column starts in %s, found %d (%s)", basename(path),
                   length(starts), paste(starts, collapse = ", ")), call. = FALSE)
    }
    # A word that runs past the next column start is a cell cut off at the
    # column edge and printed flush against the next one ("Charter
    # SchoMcclellanville"). Two passes: rows without one give the vocabulary
    # of cities, which is then used to split the joined word at the longest
    # tail that is a known city. A joined word with no such tail stops the run.
    cut_cols <- function(r, cities = NULL) {
      col <- findInterval(r$mid_x + 2, starts)
      col[col == 0L] <- 1L
      text <- r$mid_text
      joined <- which(col < 3L & r$mid_xend > starts[pmin(col + 1L, 3L)] + 1)
      if (length(joined) && is.null(cities)) return(NULL)
      for (i in joined) {
        w <- text[i]
        cut <- NA_integer_
        for (k in seq_len(nchar(w) - 1L)) {
          if (tolower(substring(w, k + 1L)) %in% cities) { cut <- k; break }
        }
        if (is.na(cut)) {
          stop(sprintf("SC: cannot split the joined cells '%s' on page %d of %s: %s", w, r$page,
                       basename(path), paste(r$mid_text, collapse = " ")), call. = FALSE)
        }
        text[i] <- substr(w, 1L, cut)
        text <- append(text, substring(w, cut + 1L), after = i)
        col <- append(col, col[i] + 1L, after = i)
        message(sprintf("SC: split '%s' into '%s' / '%s' (%s)", w, text[i], text[i + 1L],
                        basename(path)))
      }
      cell <- function(j) str_squish(paste(text[col == j], collapse = " "))
      list(school_name = cell(1L), city = cell(2L), grade_range = cell(3L))
    }
    cells <- lapply(rows, cut_cols)
    clean <- !vapply(cells, is.null, logical(1))
    cities <- unique(tolower(vapply(cells[clean], `[[`, character(1), "city")))
    cities <- cities[nzchar(cities)]
    cells[!clean] <- lapply(rows[!clean], cut_cols, cities = cities)

    tibble(
      page = vapply(rows, `[[`, integer(1), "page"),
      region = vapply(rows, `[[`, character(1), "region"),
      county = vapply(rows, `[[`, character(1), "county"),
      school_type = vapply(rows, `[[`, character(1), "school_type"),
      school_name = vapply(cells, `[[`, character(1), "school_name"),
      city = vapply(cells, `[[`, character(1), "city"),
      grade_range = vapply(cells, `[[`, character(1), "grade_range"),
      total = vapply(rows, `[[`, character(1), "total"),
      pct = vapply(rows, `[[`, character(1), "pct")
    ) %>%
      mutate(time = as.Date(school_year_time_from_end(end_year)))
  }

  report_files <- list.files("raw", pattern = "^45_day_report_\\d{4}\\.pdf$", full.names = TRUE)
  if (!length(report_files)) stop("SC: no 45-day reports in raw/ to process.", call. = FALSE)

  schools <- bind_rows(lapply(report_files, read_report))

  # A layout change would most likely show up as a short table. Each report
  # is a census of the state's schools, so the count is expected to be
  # stable from one year to the next.
  per_year <- schools %>% count(time)
  message(sprintf("SC: 45-day report rows per year: %s",
                  paste(sprintf("%s=%d", format(per_year$time, "%Y"), per_year$n), collapse = ", ")))
  if (any(per_year$n < 1000L)) {
    stop("SC: a 45-day report parsed to fewer than 1000 schools: ",
         paste(sprintf("%s=%d", format(per_year$time, "%Y"), per_year$n), collapse = ", "),
         call. = FALSE)
  }
  if (max(per_year$n) / min(per_year$n) > 1.05) {
    stop("SC: school counts differ by more than 5% between reports: ",
         paste(sprintf("%s=%d", format(per_year$time, "%Y"), per_year$n), collapse = ", "),
         call. = FALSE)
  }
  blank <- schools %>% summarise(school_name = sum(school_name == ""), city = sum(city == ""),
                                 grade_range = sum(grade_range == ""))
  if (blank$school_name > 0) {
    stop("SC: ", blank$school_name, " school row(s) without a name", call. = FALSE)
  }
  if (blank$city + blank$grade_range > 0) {
    message(sprintf("SC: %d row(s) with no city, %d with no grade range", blank$city,
                    blank$grade_range))
  }

  # "Lexington/Richland" is one school district straddling two counties; the
  # rows keep the label with no FIPS.
  school_rows <- schools %>%
    mutate(county = resolve_county(county)) %>%
    join_county_fips("SC", no_fips = "Lexington/Richland") %>%
    transmute(
      time, geography, geography_name, type = "school",
      school_name, school_type, city, grade_range, grade = "Overall",
      N_enrolled = pdf_number(total),
      flag_enrolled = censor_flag(total),
      # ">96%" and "<5%" are bounds: the column holds the bound, the flag says
      # which way the true value lies
      pct_utd = pdf_number(pct),
      flag_utd = censor_flag(pct)
    )

  # ---- County exemptions (five-year report) ---------------------------------
  raw_path <- "./raw/SC_2019-23.csv"
  header_lines <- readr::read_lines(raw_path, n_max = 3)

  header_year <- readr::read_csv(I(header_lines[2]), col_names = FALSE, show_col_types = FALSE)
  header_metric <- readr::read_csv(I(header_lines[3]), col_names = FALSE, show_col_types = FALSE)
  h2 <- as.character(header_year[1, ])
  h3 <- as.character(header_metric[1, ])
  h2[h2 == "NA"] <- NA_character_
  year_fill <- character(length(h2))
  current_year <- NA_character_
  for (i in seq_along(h2)) {
    if (!is.na(h2[i]) && h2[i] != "") current_year <- h2[i]
    year_fill[i] <- current_year
  }

  col_names <- vapply(seq_along(h2), function(i) {
    a <- h2[i]
    b <- h3[i]
    if (!is.na(a) && a %in% c("State", "County")) return(a)
    if (is.na(a) || a == "") a <- year_fill[i]
    if (is.na(a) || a == "") return(paste0("col", i))
    metric_key <- if (!is.na(b) && b != "") {
      dplyr::case_when(
        str_detect(b, "Enrolled") ~ "Enrolled",
        str_detect(b, "%|Percent") ~ "ExemptPercent",
        str_detect(b, "#|Number") ~ "ExemptCount",
        TRUE ~ b
      )
    } else {
      "value"
    }
    nm <- paste0(a, "_", metric_key)
    nm <- str_replace_all(nm, "\\s+", "_")
    nm <- str_replace_all(nm, "[^A-Za-z0-9_\\-]+", "_")
    nm <- str_replace_all(nm, "_+", "_")
    str_replace_all(nm, "_$", "")
  }, character(1))

  data_raw <- readr::read_csv(
    raw_path,
    skip = 3,
    col_names = col_names,
    show_col_types = FALSE
  ) %>%
    mutate(State = if_else(State == "", NA_character_, State)) %>%
    tidyr::fill(State, .direction = "down") %>%
    rename(state = State, county = County) %>%
    mutate(across(-c(state, county), as.character))

  data_long <- data_raw %>%
    pivot_longer(
      cols = -c(state, county),
      names_to = "year_metric",
      values_to = "value"
    ) %>%
    mutate(
      year = str_extract(year_metric, "^\\d{4}-\\d{4}"),
      metric = str_remove(year_metric, "^\\d{4}-\\d{4}_"),
      value = readr::parse_number(as.character(value))
    ) %>%
    filter(!is.na(year))

  data_wide <- data_long %>%
    select(state, county, year, metric, value) %>%
    pivot_wider(names_from = metric, values_from = value)

  county_rows <- data_wide %>%
    mutate(county = str_trim(county)) %>%
    join_county_fips(
      "SC",
      statewide = c("State Totals", "State Total", "Total", "Statewide")
    ) %>%
    transmute(
      time = as.Date(school_year_time_from_end(sub(".*-", "", year))),
      geography, geography_name,
      type = if_else(nchar(geography) == 2L, "state", "county"),
      grade = "Overall",
      N_personal_exempt = ExemptCount,
      pct_personal_exempt = ExemptPercent,
      N_enrolled = Enrolled
    )

  # ---- Assemble ----------------------------------------------------------------
  data_out <- bind_rows(county_rows, school_rows) %>%
    select(time, geography, geography_name, type, school_name, school_type, city,
           grade_range, grade, N_enrolled, flag_enrolled, pct_utd, flag_utd,
           N_personal_exempt, pct_personal_exempt) %>%
    arrange(time, factor(type, levels = c("state", "county", "school")),
            geography_name, school_name)

  message(sprintf(
    "SC: %d rows (%d state, %d county, %d school), school years %s to %s",
    nrow(data_out), sum(data_out$type == "state"), sum(data_out$type == "county"),
    sum(data_out$type == "school"), min(data_out$time), max(data_out$time)))

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(data_out, "South Carolina", "./standard/data.csv.gz", from = "percent")
  update_latest_year(latest_school_year(school_rows), ids = "dph_45_day_reports")
  update_latest_year(latest_school_year(county_rows), ids = "dph_county_page")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
