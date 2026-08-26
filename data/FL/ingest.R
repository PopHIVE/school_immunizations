library(dcf)
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(vroom)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")

# FLHealthCHARTS serves "Immunization Levels in Kindergarten" (cid=75) --
# percent of kindergarten students with proper immunization documentation, by
# county, 2007-2026 -- through an ASP.NET report viewer with no plain CSV
# download. The per-county trend view (NonVitalIndNoGrp.CntyGrid) needs
# session state from the dashboard page and 404s/errors when queried
# directly ("Unable to find the DataLayer for DataLayer.Linked
# DL_For_TempVariables"). The "Ten Year Report" (NonVitalIndNoGrp.TenYrsRpt)
# is a single flat grid of every county for a 10-year window and IS reachable
# statelessly, including its Export-to-Excel link, so that is what gets
# fetched here -- twice, since the source only ever returns 10 years at a
# time. `drpYear` sets the window's END year; the default (omitted) call
# returns the most recent 10 years, and the second call asks for
# (that max year - 10) to get the next window back. FDOH's own earliest year
# for this indicator is 2007, so two windows always cover the full series
# without a gap, this year or in any future one.
kg_query <- function(drp_year = NULL) {
  params <- c(
    rdReport             = "NonVitalIndNoGrp.TenYrsRpt",
    cid                  = "75",
    rdReportFormat       = "NativeExcel",
    rdExportTableID      = "dtTenYrsDataGrid",
    rdShowGridlines      = "True",
    rdHasWaitPanel       = "True",
    rdExcelOutputFormat  = "Excel2007",
    rdExportFilename     = "NonVitalInd_TenYrsReport.xlsx"
  )
  if (!is.null(drp_year)) params["drpYear"] <- as.character(drp_year)
  paste0(
    "https://www.flhealthcharts.gov/ChartsDashboards/rdPage.aspx?",
    paste(names(params), unname(params), sep = "=", collapse = "&")
  )
}

# Download to a temp file and only move it into raw/ once it is there and
# readable, so a blocked/failed request cannot truncate the copy already on
# disk (see OR/ingest.R for the incident that made this the pattern).
download_kg_xlsx <- function(url, dest) {
  tmp <- tempfile(fileext = ".xlsx")
  ok <- tryCatch({
    download.file(url, tmp, mode = "wb", quiet = TRUE)
    readxl::excel_sheets(tmp)
    TRUE
  }, error = function(e) FALSE, warning = function(w) FALSE)

  if (ok) {
    file.copy(tmp, dest, overwrite = TRUE)
  } else if (!file.exists(dest)) {
    stop("FL: could not download ", url, " and there is no ", dest,
         " to fall back on.")
  } else {
    message("FL: download failed for ", url, "; keeping existing ", dest)
  }
  unlink(tmp)
}

dir.create("raw", showWarnings = FALSE)
kg_recent_path <- "raw/FL_kindergarten_recent10yr.xlsx"
kg_early_path  <- "raw/FL_kindergarten_early10yr.xlsx"

download_kg_xlsx(kg_query(), kg_recent_path)
kg_recent_years <- suppressWarnings(as.integer(as.character(
  unlist(readxl::read_excel(kg_recent_path, col_names = FALSE)[2, ])
)))
kg_max_year <- max(kg_recent_years, na.rm = TRUE)
download_kg_xlsx(kg_query(kg_max_year - 10L), kg_early_path)

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

parse_exempt <- function(x) {
  x <- str_squish(as.character(x))
  # Keep suppressed values ("<5") as NA so we do not overstate exemptions.
  x <- if_else(str_detect(x, "^<"), NA_character_, x)
  suppressWarnings(as.numeric(str_replace_all(x, ",", "")))
}

# The Ten Year Report grid is wide: one "County" column, then a Count/Percent
# column pair per year, with the year label sitting only above the Count
# column (row 2) and "Florida"/county names starting at row 4.
parse_kg_tenyear <- function(path) {
  raw <- readxl::read_excel(path, col_names = FALSE)
  year_row <- as.character(unlist(raw[2, ]))
  body <- raw[-(1:3), ]
  county <- as.character(body[[1]])

  year_cols <- seq(2, ncol(raw), by = 2)
  pieces <- lapply(year_cols, function(i) {
    yr <- suppressWarnings(as.integer(year_row[i]))
    if (is.na(yr)) return(NULL)
    tibble(
      county = county,
      data_year = yr,
      N_complete = clean_numeric(body[[i]]),
      pct_complete = clean_numeric(body[[i + 1]])
    )
  })
  bind_rows(pieces)
}

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # Every scraped file is census-tract level with the same five columns:
  # tract, county, state, population aged 4-18, exemptions aged 4-18. Only the
  # HEADERS differ -- the 23-county roll-up and 39 of the per-county files use
  # "Census,County,State,TotalPop4_18yrs,TotalExempt4_18yrs", while five
  # (Martin, Union, Volusia, Walton, Washington) carry a truncated
  # "attributes.*" header that names only three of the five columns. Reading
  # positionally with skip = 1 handles both. The roll-up and the per-county
  # files cover disjoint counties (23 + 44 = all 67), so there is no overlap.
  scraped_dir <- "raw/Florida Vaccine Exemption Data (Scraped)"
  fl_cols <- c("census", "county", "state", "total_pop_4_18", "total_exempt_4_18")

  read_scraped <- function(path) {
    readr::read_csv(
      path,
      skip           = 1,
      col_names      = fl_cols,
      col_types      = readr::cols(.default = readr::col_character()),
      show_col_types = FALSE,
      progress       = FALSE
    )
  }

  scraped_files <- list.files(scraped_dir, pattern = "\\.csv$", full.names = TRUE)
  fl_raw <- bind_rows(lapply(scraped_files, read_scraped))

  fl_clean <- fl_raw %>%
    transmute(
      county = str_squish(county),
      total_pop_4_18 = suppressWarnings(
        as.numeric(str_replace_all(str_squish(total_pop_4_18), ",", ""))
      ),
      total_exempt_4_18 = parse_exempt(total_exempt_4_18)
    ) %>%
    filter(!is.na(county), county != "") %>%
    group_by(county) %>%
    summarize(
      total_pop_4_18 = sum(total_pop_4_18, na.rm = TRUE),
      total_exempt_4_18 = sum(total_exempt_4_18, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      time = as.Date("2024-09-01"),
      pct_full_exempt = if_else(
        total_pop_4_18 > 0,
        (total_exempt_4_18 / total_pop_4_18) * 100,
        NA_real_
      )
    )

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fl_fips <- all_fips %>%
    filter(state == "FL", nchar(geography) == 5) %>%
    transmute(
      geography,
      geography_name
    )

  fl_joined <- fl_clean %>%
    left_join(fl_fips, by = c("county" = "geography_name"))

  # Report counties that failed the FIPS join instead of dropping them silently.
  unmatched <- sort(unique(fl_joined$county[is.na(fl_joined$geography)]))
  if (length(unmatched)) {
    warning(length(unmatched), " county name(s) matched no FL FIPS and were dropped: ",
            paste(unmatched, collapse = ", "), call. = FALSE)
  }

  data_out <- fl_joined %>%
    mutate(
      grade = "Overall",
      N_dtap = NA_real_,
      N_polio = NA_real_,
      N_mmr = NA_real_,
      N_hep_b = NA_real_,
      N_varicella = NA_real_,
      N_personal_exempt = NA_real_,
      N_medical_exempt = NA_real_,
      N_full_exempt = total_exempt_4_18,
      pct_dtap = NA_real_,
      pct_polio = NA_real_,
      pct_mmr = NA_real_,
      pct_hep_b = NA_real_,
      pct_varicella = NA_real_,
      pct_personal_exempt = NA_real_,
      pct_medical_exempt = NA_real_
    ) %>%
    filter(!is.na(geography)) %>%
    transmute(
      time,
      geography,
      # FDOH labels these "Alachua County"; the standard label is the bare name,
      # as join_county_fips() emits for the states that use it.
      geography_name = sub("\\s+County$", "", county),
      grade,
      N_dtap,
      N_polio,
      N_mmr,
      N_hep_b,
      N_varicella,
      N_personal_exempt,
      N_medical_exempt,
      N_full_exempt,
      pct_dtap,
      pct_polio,
      pct_mmr,
      pct_hep_b,
      pct_varicella,
      pct_personal_exempt,
      pct_medical_exempt,
      pct_full_exempt,
      total_pop_4_18
    )

  # Kindergarten immunization levels (cid=75): percent of kindergarten
  # students with proper immunization documentation, by county and year.
  # "Data Year" is the school year's END year (2026 already carries a full
  # denominator as of this run, i.e. the completed 2025-26 school year), so
  # it is dated with school_year_time_from_end() like every other state.
  kg_long <- bind_rows(
    parse_kg_tenyear(kg_recent_path),
    parse_kg_tenyear(kg_early_path)
  ) %>%
    filter(county != "Florida")

  kg_dupes <- kg_long %>% count(county, data_year) %>% filter(n > 1)
  if (nrow(kg_dupes)) {
    stop("FL: kindergarten data has ", nrow(kg_dupes), " duplicate county/year ",
         "combination(s) across the two Ten Year Report windows -- the windows ",
         "are no longer disjoint.", call. = FALSE)
  }

  kg_joined <- kg_long %>%
    mutate(county_key = paste0(county, " County")) %>%
    left_join(fl_fips, by = c("county_key" = "geography_name"))

  unmatched_kg <- sort(unique(kg_joined$county[is.na(kg_joined$geography)]))
  if (length(unmatched_kg)) {
    warning(length(unmatched_kg), " county name(s) in the kindergarten data ",
            "matched no FL FIPS and were dropped: ",
            paste(unmatched_kg, collapse = ", "), call. = FALSE)
  }

  kg_out <- kg_joined %>%
    filter(!is.na(geography)) %>%
    transmute(
      time = as.Date(school_year_time_from_end(data_year)),
      geography,
      geography_name = county,
      grade = "Kindergarten",
      N_complete,
      pct_complete
    )

  # Following the convention in data/CA/ingest.R: the exemption cohort (grade
  # "Overall") and the kindergarten cohort stack into ONE canonical file,
  # standard/data.csv.gz -- the name scripts/build_all_states_county_standard.R
  # and scripts/generate_measure_info.R both read by -- with two narrower
  # files beside it that each drop the other cohort's structurally empty
  # columns, so a consumer of just one cohort is not carrying the other's
  # all-NA measure columns.
  EXEMPT_GRADES <- "Overall"
  KG_GRADES <- "Kindergarten"

  dir.create("standard", showWarnings = FALSE)
  combined <- bind_rows(data_out, kg_out) %>% arrange(time, grade, geography)

  # write_standard() converts pct_* -> rate_*, canonicalises count names, and
  # drops any column empty across the WHOLE combined file. The returned frame
  # is reused for the two narrower files below rather than rebuilt from
  # data_out/kg_out, so all three files agree on every value they share.
  wide <- write_standard(combined, "Florida", "standard/data.csv.gz",
                          from = "percent")

  # `from = "rate"` here is a no-op conversion -- `wide`'s pct_ columns are
  # already renamed to rate_ -- so the only thing re-running write_standard()
  # does for these two is drop_empty_measures() on just the filtered subset.
  write_standard(wide %>% filter(grade %in% EXEMPT_GRADES),
                  "Florida (exemptions)",
                  "standard/data_exemptions.csv.gz", from = "rate")
  write_standard(wide %>% filter(grade %in% KG_GRADES),
                  "Florida (kindergarten)",
                  "standard/data_kindergarten.csv.gz", from = "rate")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
