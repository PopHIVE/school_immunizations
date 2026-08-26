source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
# =============================================================================
# WV - Exemption Counts by County, School Year, and Grade (Multiple Vaccines)
# =============================================================================

library(dplyr)
library(readxl)
library(stringr)
library(tidyr)
library(purrr)
library(vroom)

if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

raw_file <- "raw/Exempetion Data Request Final 9.2.25 (1).xls"
raw_state <- list(
  hash = tools::md5sum(raw_file),
  script = tools::md5sum("ingest.R"),
  force = "2026-02-05"
)

script_hash <- as.character(tools::md5sum("ingest.R"))

# Gated on the script as well as the data, like every other state, so an
# edit to the parsing below is actually applied to standard/.
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "WV") %>%
    # Bare county names, matching join_county_fips() in the other states.
    mutate(geography_name = sub(" County$", "", geography_name)) %>%
    select(geography, geography_name, state)

  sheet_map <- tibble::tribble(
    ~sheet, ~vaccine,
    "MMR cnty", "mmr",
    "DTaP cnty", "dtap",
    "Tdap cnty", "tdap",
    "Hib cnty", "hib",
    "Var cnty", "varicella",
    "Men cnty", "menacwy",
    "IPV cnty", "ipv",
    "PCV cnty", "pcv",
    "Hep cnty", "hep"
  )

  # Every sheet's SECOND table is read as well, as grade "all, permanent".
  # Named rather than assumed, so a sheet that stops carrying one fails the
  # check below instead of silently dropping out of the stratum.
  PERMANENT_VACCINES <- sheet_map$vaccine

  # Each vaccine sheet holds TWO stacked tables, and three side-by-side blocks
  # within the first:
  #
  #   rows 2..58   header "COUNTY | 2017-2018 | ... | Total", three blocks side
  #                by side (Kindergarten, 7th grade, 12th grade), one exemption
  #                COUNT per county and school year, ending in a Total row
  #   rows ~60..   a DIFFERENT table, "Total <vax> Permanent Medical Exemptions
  #                per Year": one row per county, keyed by single CALENDAR years
  #                2017..2025, counting only PERMANENT MEDICAL exemptions and not
  #                split by grade
  #
  # Both the blocks and the row bounds are located from the sheet rather than
  # hard-coded, because the previous version's fixed offsets were wrong twice:
  #
  #   * It read rows 3:nrow(raw), running off the bottom of the first table and
  #     pulling the lower table into the Kindergarten block -- relabelled with
  #     the first table's school years, so one county's standing total became its
  #     count in every year. Barbour's kindergarten MMR count is 0 in every year,
  #     and came out as 2 in all of them.
  #   * It assumed the blocks start at columns 1, 12 and 23. Tdap has nine school
  #     years rather than eight, so its blocks start at 1, 13 and 25, and the
  #     7th- and 12th-grade reads were a column out.
  #
  # The lower table is read as its own grade stratum, "all, permanent", rather
  # than being stacked with the by-grade counts above it. It is a different
  # measure on a different clock, and a STANDING count rather than a per-year
  # addition: the values mostly ratchet upwards (Berkeley 4,4,5,5,5,7,9,10,10)
  # but do fall when an exemption lapses (Boone 1,1,1,1,1,1,1,0,0; Wood
  # 2,2,2,2,2,1,0,0,0), so each cell is the number of permanent medical
  # exemptions in force that year -- not a cumulative sum, and not additive with
  # the kindergarten/7th/12th counts, which it partly duplicates.
  #
  # Its calendar year is mapped to the school year that STARTS in it: 2017 ->
  # "2017-09-01", the same date as school year 2017-2018 above. The workbook
  # gives no month, and the alternative -- a January date -- would invent time
  # values nothing else in the project shares (see resources/school_year.R).
  grade_from_title <- function(title) {
    t <- tolower(paste(title, collapse = " "))
    if (grepl("kindergarten", t)) return("Kindergarten")
    if (grepl("\\b7(th)?\\b", t)) return("7th Grade")
    if (grepl("\\b12(th)?\\b", t)) return("12th Grade")
    NA_character_
  }

  parse_sheet <- function(sheet, vaccine) {
    raw <- as.data.frame(read_excel(raw_file, sheet = sheet, col_names = FALSE,
                                    .name_repair = "minimal"))

    hdr <- which(raw == "COUNTY", arr.ind = TRUE)
    if (!nrow(hdr)) stop("WV: no COUNTY header on sheet '", sheet, "'")

    # The by-school-year table is the topmost one; any lower COUNTY header
    # belongs to the permanent per-year table and bounds where this one stops.
    hrow <- min(hdr[, "row"])
    block_cols <- sort(hdr[hdr[, "row"] == hrow, "col"])
    header <- trimws(as.character(unlist(raw[hrow, ])))

    c1 <- trimws(as.character(raw[[1]]))
    stop_rows <- c(
      which(!is.na(c1) & grepl("^total$", c1, ignore.case = TRUE)),
      unique(hdr[hdr[, "row"] > hrow, "row"]) - 1L
    )
    stop_rows <- stop_rows[stop_rows > hrow]
    if (!length(stop_rows)) stop("WV: no Total row after the header on '", sheet, "'")
    last_row <- min(stop_rows) - 1L

    blocks <- lapply(seq_along(block_cols), function(i) {
      ccol <- block_cols[i]
      end_col <- if (i < length(block_cols)) block_cols[i + 1] - 1L else ncol(raw)
      # Value columns are the ones headed by a school year; this drops the
      # block's "Total" column and any spacer.
      idx <- seq_len(ncol(raw))
      year_cols <- idx[idx > ccol & idx <= end_col &
                       grepl("^\\d{4}\\s*-\\s*\\d{4}$", header)]
      if (!length(year_cols)) {
        stop("WV: no school-year columns in the block at column ", ccol,
             " of '", sheet, "'")
      }

      grade <- grade_from_title(raw[hrow - 1L, ccol:end_col])
      if (is.na(grade)) {
        stop("WV: could not read a grade from the title above column ", ccol,
             " of '", sheet, "'")
      }

      body <- raw[(hrow + 1L):last_row, c(ccol, year_cols), drop = FALSE]
      names(body) <- c("COUNTY", header[year_cols])
      body %>%
        mutate(across(everything(), as.character)) %>%
        filter(!is.na(COUNTY)) %>%
        pivot_longer(cols = -COUNTY, names_to = "school_year",
                     values_to = "n_exempt") %>%
        mutate(
          time = school_year_time_from_end(
            school_year_end_from_label(str_trim(school_year))),
          grade = grade,
          vaccine = vaccine
        ) %>%
        select(COUNTY, time, grade, vaccine, n_exempt)
    })

    bind_rows(blocks)
  }

  # The lower table: "Total <vax> Permanent Medical Exemptions per Year", one
  # block starting at column 1, keyed by calendar year. Located the same way as
  # the first table -- from its own COUNTY header and its own Total row -- so an
  # added year or a shifted row does not silently move the read.
  parse_permanent <- function(sheet, vaccine) {
    raw <- as.data.frame(read_excel(raw_file, sheet = sheet, col_names = FALSE,
                                    .name_repair = "minimal"))

    hdr <- which(raw == "COUNTY", arr.ind = TRUE)
    if (!nrow(hdr)) stop("WV: no COUNTY header on sheet '", sheet, "'")
    lower <- unique(hdr[hdr[, "row"] > min(hdr[, "row"]), "row"])
    if (!length(lower)) {
      stop("WV: no second COUNTY header on sheet '", sheet,
           "', so no permanent medical exemptions table to read.")
    }
    hrow <- min(lower)

    # Read the title rather than trusting the position: the by-grade table on
    # the DTaP sheet is itself titled "DTaP Permanent Medical Exemptions", so
    # "the lower table" has to be confirmed to be the per-year one.
    title <- tolower(paste(stats::na.omit(as.character(unlist(raw[hrow - 1L, ]))),
                           collapse = " "))
    if (!grepl("permanent medical exemptions", title) ||
        !grepl("per\\s*year", title)) {
      stop("WV: the table at row ", hrow, " of '", sheet, "' is titled '", title,
           "', not the permanent medical exemptions per year table.")
    }

    header <- trimws(as.character(unlist(raw[hrow, ])))
    year_cols <- which(grepl("^\\d{4}$", header))
    year_cols <- year_cols[year_cols > 1L]
    if (!length(year_cols)) {
      stop("WV: no calendar-year columns in the permanent table of '", sheet, "'")
    }

    c1 <- trimws(as.character(raw[[1]]))
    totals <- which(!is.na(c1) & grepl("^total$", c1, ignore.case = TRUE))
    totals <- totals[totals > hrow]
    if (!length(totals)) {
      stop("WV: no Total row after the permanent header on '", sheet, "'")
    }
    last_row <- min(totals) - 1L

    body <- raw[(hrow + 1L):last_row, c(1L, year_cols), drop = FALSE]
    names(body) <- c("COUNTY", header[year_cols])
    body %>%
      mutate(across(everything(), as.character)) %>%
      filter(!is.na(COUNTY)) %>%
      pivot_longer(cols = -COUNTY, names_to = "calendar_year",
                   values_to = "n_exempt") %>%
      mutate(
        # Calendar year -> the school year starting in it; see the note above.
        time = school_year_time(as.integer(calendar_year)),
        grade = "all, permanent",
        vaccine = vaccine
      ) %>%
      select(COUNTY, time, grade, vaccine, n_exempt)
  }

  permanent_map <- sheet_map[sheet_map$vaccine %in% PERMANENT_VACCINES, ]
  if (nrow(permanent_map) != length(PERMANENT_VACCINES)) {
    stop("WV: PERMANENT_VACCINES names a vaccine that is not in sheet_map: ",
         paste(setdiff(PERMANENT_VACCINES, sheet_map$vaccine), collapse = ", "))
  }

  data <- bind_rows(
    bind_rows(purrr::pmap(sheet_map, parse_sheet)),
    bind_rows(purrr::pmap(permanent_map, parse_permanent))
  ) %>%
    mutate(
      county = str_to_title(str_trim(COUNTY)),
      county = str_replace(county, "^Mcdowell$", "McDowell"),
      geography_name = county,
      n_exempt = as.numeric(n_exempt)
    ) %>%
    filter(!str_detect(county, "^Total"), county != "County") %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(!is.na(geography)) %>%
    select(
      time,
      geography,
      geography_name,
      grade,
      vaccine,
      n_exempt
    ) %>%
    # standard/ output is wide: one column per measure, the vaccine in the column
    # NAME rather than in a `vaccine` value column. Long-by-vaccine also made the
    # rows non-unique on (geography, time, grade), and anything summing across
    # that stratum to get a county total would count a pupil once per vaccine
    # they are exempt from.
    #
    # WVDHHR publishes an exemption COUNT per vaccine and no enrolment, so there
    # is no denominator to turn these into rates and no overall exempt total:
    # a pupil exempt from three vaccines appears in three of these columns.
    mutate(vaccine = paste0("N_", vaccine, "_exempt"))

  # Every column header the parsers accept carries a parsable year, so an NA
  # `time` here means a header was matched that is not one.
  if (any(is.na(data$time))) {
    stop("WV: ", sum(is.na(data$time)), " row(s) got no time from their column ",
         "header -- the sheet was misread.")
  }

  # One value per (county, year, grade, vaccine) before the pivot.
  #
  # Checked rather than reduced with values_fn: an aggregating values_fn is what
  # let the parser's off-the-bottom read pass silently, adding a county's
  # cumulative total onto its yearly count. A second value here means the sheet
  # was misread, and the build should say so.
  dup_long <- data %>%
    count(geography, time, grade, vaccine) %>%
    filter(n > 1)
  if (nrow(dup_long)) {
    stop("WV: ", nrow(dup_long), " (county, year, grade, vaccine) combination(s) ",
         "appear more than once, e.g. ", dup_long$geography[1], " ",
         dup_long$time[1], " ", dup_long$grade[1], " ", dup_long$vaccine[1],
         " x", dup_long$n[1], " -- the sheet was misread.")
  }

  data <- data %>%
    pivot_wider(names_from = vaccine, values_from = n_exempt) %>%
    arrange(time, geography, grade)

  dir.create("standard", showWarnings = FALSE)
  write_standard(data, "West Virginia", "standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
