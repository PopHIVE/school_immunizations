source("../../resources/rate_scale.R")
source("../../resources/county_fips.R")
source("../../resources/school_year.R")
# =============================================================================
# OH - MMR Exemption Rate (Kindergarten) by County
#
# The ODH file is a map graphic, not a table: the top of the sheet is the map's
# county callouts, and the tabular data sits below it in TWO side-by-side
# County/rate blocks (columns 1+9 and 19+27), 44 counties each. Reading only the
# first block -- which the previous version did, by taking the single
# `which(raw[[1]] == "County")` header -- silently dropped 44 of Ohio's 88
# counties. Every County/rate header pair on the sheet is located and stacked
# instead, so a re-laid-out workbook is caught by the county count rather than
# by half the state going missing.
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

raw_file <- "raw/Ohio % of Students with MMR Exemption.xlsx"
raw_state <- list(hash = tools::md5sum(raw_file))

script_hash <- as.character(tools::md5sum("ingest.R"))

# Gated on the script as well as the data, like every other state, so an
# edit to the parsing below is actually applied to standard/.
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  raw <- read_excel(raw_file, col_names = FALSE, .name_repair = "minimal")
  raw <- as.data.frame(raw)

  # ---- School year, read from the sheet's own title -------------------------
  # "... By School County, 2024-2025 School Year". Parsed rather than typed so
  # next year's file cannot keep this year's date.
  title <- paste(na.omit(unlist(raw[seq_len(min(5, nrow(raw))), ])), collapse = " ")
  yr <- str_match(title, "(\\d{4})\\s*-\\s*(\\d{4})\\s*School Year")
  if (is.na(yr[1, 1])) {
    stop("No 'YYYY-YYYY School Year' label found in the title rows of ", raw_file)
  }
  time_value <- school_year_time_from_end(yr[1, 3])

  # ---- Locate every County / rate header pair -------------------------------
  header_cells <- which(raw == "County", arr.ind = TRUE)
  if (!nrow(header_cells)) stop("No 'County' header cell found in ", raw_file)

  rate_cells <- which(raw == "MMR Exemption Rate (%)", arr.ind = TRUE)
  if (nrow(rate_cells) != nrow(header_cells)) {
    stop(sprintf(
      "%d 'County' header(s) but %d rate header(s) in %s -- the layout changed.",
      nrow(header_cells), nrow(rate_cells), raw_file))
  }

  blocks <- lapply(seq_len(nrow(header_cells)), function(i) {
    hrow <- header_cells[i, "row"]
    ccol <- header_cells[i, "col"]
    # Both blocks share the header row, so a block's rate column is the nearest
    # rate header to the right of its County header.
    candidates <- rate_cells[rate_cells[, "row"] == hrow, "col"]
    candidates <- candidates[candidates > ccol]
    if (!length(candidates)) {
      stop("No rate column to the right of the County header at row ", hrow,
           ", column ", ccol, ".")
    }
    rcol <- min(candidates)
    body <- raw[(hrow + 1):nrow(raw), c(ccol, rcol)]
    names(body) <- c("county", "rate_raw")
    body[!is.na(body$county), , drop = FALSE]
  })

  data_raw <- bind_rows(blocks)

  # ---- Values ---------------------------------------------------------------
  # ODH prints these as Excel proportions (0.024 = 2.4%), which is already the
  # standard rate scale, so it is declared as such. The previous version instead
  # inferred it column-globally (`if (max(...) <= 1.5) 100 else 1`), the exact
  # pattern resources/rate_scale.R documents as having broken VT and RI.
  #
  # "None reported." is a withheld cell, not a zero: parse_rate() returns NA for
  # it and is_censored() records it, so the row carries a suppressed_flag rather
  # than an invented 0.
  data <- data_raw %>%
    mutate(
      county = str_squish(as.character(county)),
      rate_raw = str_squish(as.character(rate_raw)),
      # ODH's marker for a county it did not publish; not one of the standard
      # censoring markers, so it is mapped onto the undirected one.
      rate_raw = if_else(str_detect(tolower(rate_raw), "^none reported"),
                         "N/A", rate_raw),
      pct_mmr_exempt = parse_rate(rate_raw, from = "rate"),
      time = time_value,
      grade = "Kindergarten"
    ) %>%
    join_county_fips("OH", statewide = c("Ohio", "Total", "State Total")) %>%
    select(time, geography, geography_name, grade, pct_mmr_exempt) %>%
    arrange(geography)

  # Ohio has 88 counties; the file is a single kindergarten year, so anything
  # else means a block was missed or double-counted.
  n_counties <- sum(nchar(data$geography) == 5)
  if (n_counties != 88) {
    stop("Parsed ", n_counties, " OH county rows, expected 88.")
  }

  dir.create("standard", showWarnings = FALSE)
  write_standard(data, "Ohio", "standard/data.csv.gz", from = "rate")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
