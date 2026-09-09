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

# ---- Download from the CDPHE ArcGIS service ----
# Source: CDPHE Open Data, Colorado School and Child Care Immunization Data
#   (MapServer OPEN_DATA/cdphe_sccidr; also surfaced on data.colorado.gov).
#   Five tables of the service are ingested, each paged through its /query
#   endpoint into its own raw/ CSV:
#
#     1  county      -> standard/data.csv.gz (type = "county")
#     4  statewide   -> standard/data.csv.gz (type = "state", geography 08)
#     2  district    -> standard/data_districts.csv.gz
#     5  facility, 2017-18 to 2022-23  \
#     6  facility, 2023-24 to 2025-26  /  standard/data_schools.csv.gz
#
#   Paging table 1 gives the same 64,128 rows, in the same OBJECTID order and
#   with the same values, as the hub's CSV export used to. Layer 0 is county
#   boundaries and table 3 is college/university data; neither is ingested.
#
#   All five tables share one long layout: one row per geography, school
#   year (Year, "2017/2018"), survey type (Survey_Type: Child Care/Preschool,
#   Kindergarten, School), vaccine and metric, with Value_Percent and the
#   per-vaccine Enrollment. The district and facility tables add an ID and
#   the name fields; the facility tables also carry Facility_Type and County.
#
#   The two facility tables hold 1.6 million rows between them, 230 MB as
#   CSV, which is more than GitHub accepts in one file. They are fetched with
#   a `where` clause that keeps only the three metrics the parser uses
#   (Fully Immunized, Medical Exemption, Nonmedical Exemption), which brings
#   them to 80 MB and 39 MB, and the raw copies are stored gzip-compressed
#   (about 4 MB and 2 MB). The county, district and statewide tables are
#   small and are fetched whole as plain CSV, so the metrics the parser skips
#   (In Process, Incomplete Record, No Record, Summary Compliant) remain in
#   raw/ for those three.
#
# The previous version ran download.file() straight onto the committed CSV,
# and download.file() truncates its destination when the transfer fails, so a
# failed refresh destroyed the copy already in raw/ (the incident
# data/OR/ingest.R documents). arcgis_layer_csv() writes to a temporary file,
# validates it, and only then copies it over the committed one; a failed
# request warns, is recorded in process.json under fetch_state, and the
# ingest continues on the committed copy.
sources <- read_sources()
process <- dcf::dcf_process_record()

INGESTED_METRICS <- c("Fully Immunized", "Medical Exemption", "Nonmedical Exemption")
FACILITY_WHERE <- sprintf("Metric IN (%s)",
                          paste(sprintf("'%s'", INGESTED_METRICS), collapse = ","))

# Each entry's url in sources.json is the table URL (MapServer/<n>), which is
# what scripts/check_sources.R probes; arcgis_layer_csv() wants the service
# root and the index separately.
LAYERS <- list(
  county = list(id = "cdphe_county_csv",
                dest = "raw/CDPHE_Colorado_School_and_Child_Care_Immunization_County_Data_.csv",
                where = "1=1"),
  state = list(id = "cdphe_statewide_csv",
               dest = "raw/cdphe_sccidr_statewide.csv", where = "1=1"),
  district = list(id = "cdphe_district_csv",
                  dest = "raw/cdphe_sccidr_district.csv", where = "1=1"),
  facility_2017_2022 = list(id = "cdphe_facility_2017_2022_csv",
                            dest = "raw/cdphe_sccidr_facility_2017_2022.csv.gz",
                            where = FACILITY_WHERE),
  facility_2023_2025 = list(id = "cdphe_facility_2023_2025_csv",
                            dest = "raw/cdphe_sccidr_facility_2023_2025.csv.gz",
                            where = FACILITY_WHERE)
)

# sha256 of a file's content, with a .gz read decompressed, so a downloaded
# CSV can be compared with the compressed copy in raw/.
content_sha256 <- function(path) {
  con <- if (grepl("\\.gz$", path)) gzfile(path, "rb") else file(path, "rb")
  on.exit(close(con), add = TRUE)
  chunks <- list()
  repeat {
    b <- readBin(con, "raw", n = 8e6)
    if (!length(b)) break
    chunks[[length(chunks) + 1L]] <- b
  }
  digest::digest(unlist(chunks), algo = "sha256", serialize = FALSE)
}

# Fetch a table to a temporary CSV and store it gzip-compressed at dest.
# Same contract as arcgis_layer_csv(): a failure is a warning that keeps the
# committed .gz, and a stop only when there is no committed copy. The
# temporary dest has no committed copy from arcgis_layer_csv()'s point of
# view, so its stop() is caught here and turned into that warning.
#
# The .gz is rewritten only when the downloaded content differs from what
# it holds. R's gzfile() writes a zero mtime, so the same content always
# compresses to the same bytes and raw_state_md5() stays stable.
arcgis_layer_csv_gz <- function(service, layer, dest, where, page_size) {
  base <- sprintf("%s/%d/query", service, as.integer(layer))
  label <- basename(dest)
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp), add = TRUE)
  rec <- tryCatch(
    arcgis_layer_csv(service, layer = layer, dest = tmp, where = where,
                     page_size = page_size),
    error = function(e) e
  )
  if (inherits(rec, "error")) {
    msg <- conditionMessage(rec)
    if (file.exists(dest)) {
      warning(sprintf("arcgis: %s could not be refreshed from %s (%s); keeping the committed copy",
                      label, base, msg), call. = FALSE)
      return(fetch_record(base, dest, "failed", bytes = file.size(dest),
                          sha256 = fetch_sha256(dest), attempts = 1L, error = msg))
    }
    stop(sprintf("arcgis: %s could not be downloaded from %s (%s) and there is no committed copy to fall back on",
                 label, base, msg), call. = FALSE)
  }
  have <- file.exists(dest)
  if (have && identical(content_sha256(tmp), content_sha256(dest))) {
    return(fetch_record(base, dest, "unchanged", http_status = 200L,
                        bytes = file.size(dest), sha256 = fetch_sha256(dest),
                        attempts = 1L))
  }
  tmp_gz <- tempfile(fileext = ".csv.gz")
  on.exit(unlink(tmp_gz), add = TRUE)
  con_in <- file(tmp, "rb")
  con_out <- gzfile(tmp_gz, "wb")
  repeat {
    b <- readBin(con_in, "raw", n = 8e6)
    if (!length(b)) break
    writeBin(b, con_out)
  }
  close(con_in)
  close(con_out)
  dir.create(dirname(dest), showWarnings = FALSE, recursive = TRUE)
  if (!file.copy(tmp_gz, dest, overwrite = TRUE)) {
    stop("arcgis: could not write ", dest, call. = FALSE)
  }
  message(sprintf("fetch: %s %s (%s bytes compressed)", if (have) "updated" else "fetched",
                  label, format(file.size(dest), big.mark = ",")))
  fetch_record(base, dest, "updated", http_status = 200L, bytes = file.size(dest),
               sha256 = fetch_sha256(dest), attempts = 1L)
}

dir.create("raw", showWarnings = FALSE)
# A dest that is no longer in LAYERS (the facility tables used to be stored
# as plain CSV) would otherwise keep its last record in fetch_state forever.
process$fetch_state <- process$fetch_state[
  names(process$fetch_state) %in% vapply(LAYERS, `[[`, character(1), "dest")]
for (nm in names(LAYERS)) {
  l <- LAYERS[[nm]]
  src <- source_entry(sources, l$id)
  layer_url <- sub("/+$", "", src$url)
  service <- sub("/\\d+$", "", layer_url)
  layer <- as.integer(sub(".*/", "", layer_url))
  if (is.na(layer) || identical(service, layer_url)) {
    stop("CO: sources.json url for ", l$id, " must end in the table index (MapServer/<n>): ",
         src$url, call. = FALSE)
  }
  # The server's maxRecordCount is 1,760,000, so 50,000-row pages are well
  # inside it and take the facility tables in a few dozen requests rather
  # than several hundred.
  rec <- if (grepl("\\.gz$", l$dest)) {
    arcgis_layer_csv_gz(service, layer = layer, dest = l$dest,
                        where = l$where, page_size = 50000L)
  } else {
    arcgis_layer_csv(service, layer = layer, dest = l$dest,
                     where = l$where, page_size = 50000L)
  }
  process <- record_fetch(process, rec)
}

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  read_layer <- function(path) {
    d <- readr::read_csv(path, show_col_types = FALSE,
                         col_types = readr::cols(.default = readr::col_character(),
                                                 Value_Percent = readr::col_double(),
                                                 Enrollment = readr::col_double()))
    # The live API names this column "Year"; older manual exports used "Year_".
    if ("Year" %in% names(d) && !("Year_" %in% names(d))) {
      d <- dplyr::rename(d, Year_ = Year)
    }
    dplyr::select(d, -OBJECTID)
  }

  # CDPHE's `Metric` is folded into the COLUMN NAMES, not carried as a column.
  # The output is fully wide: one row per geography/year/grade, and one column
  # per vaccine x metric, so a column means the same thing on every row of the
  # file.
  #
  # Carrying `metric` as a column instead would make rate_mmr coverage on a
  # "fully_immunized" row and an MMR exemption rate on the other two -- a column
  # whose meaning depends on the row cannot be described by one piece of
  # metadata, and it stacks coverage on top of exemptions in one series for
  # anyone who does not think to filter on the metric.
  #
  # The two naming halves are deliberately different:
  #
  #   Fully Immunized -> rate_<vaccine>                    (e.g. rate_mmr)
  #   the exemptions  -> rate_<vaccine>_<metric>           (rate_mmr_medical_exempt)
  #
  # "Fully Immunized" per vaccine IS that vaccine's coverage, so it takes the
  # canonical coverage name the measure dictionary already defines and every
  # other state already uses -- rate_mmr means the same thing in Colorado as in
  # Nevada. Inventing rate_mmr_fully_immunized alongside it would give coverage
  # two spellings and let their descriptions drift apart.
  #
  # The Metric labels changed over time: older manual exports ran the words
  # together, the live API spaces them. Both spellings map to the same key.
  METRIC_KEYS <- c(
    "MedicalExemptions"    = "medical_exempt",
    "Medical Exemption"    = "medical_exempt",
    "NonMedicalExemptions" = "nonmedical_exempt",
    "Nonmedical Exemption" = "nonmedical_exempt",
    "FullyImmunized"       = "coverage",
    "Fully Immunized"      = "coverage"
  )

  # Column order for the measures. pivot_wider() emits them in first-appearance
  # order, which interleaves them arbitrarily -- rate_hep_b_nonmedical_exempt,
  # rate_pcv, rate_dtap, rate_covid_medical_exempt, ... Ordering by vaccine
  # instead keeps a vaccine's coverage and its two exemption rates together.
  # Anything not listed still survives, via everything() below.
  VACCINE_ORDER <- c("mmr", "dtap", "tdap", "polio", "hep_b", "varicella",
                     "hib", "pcv", "covid")

  # Index columns first, then the measures grouped by vaccine. Named on the
  # pct_ spelling because write_standard() renames pct_ -> rate_ afterwards,
  # preserving this order.
  order_columns <- function(data, index_cols) {
    data %>%
      select(
        all_of(index_cols),
        any_of(as.vector(rbind(
          paste0("pct_", VACCINE_ORDER),
          paste0("pct_", VACCINE_ORDER, "_medical_exempt"),
          paste0("pct_", VACCINE_ORDER, "_nonmedical_exempt")
        ))),
        any_of(paste0("N_", VACCINE_ORDER, "_enrolled")),
        # A vaccine CDPHE adds later is not in VACCINE_ORDER and would be
        # dropped silently by a bare select; this keeps it, at the end.
        everything()
      )
  }

  # Long CDPHE table -> one wide row per `id_cols` x time x grade. The same
  # function serves all five tables; they differ only in which columns
  # identify a row.
  #
  # Not ingested: "In Process", "Incomplete Record" and "No Record" (the
  # non-compliance breakdown), and "Summary Compliant" (an all-vaccine
  # compliance rate, published with a blank Vaccine so it does not fit the
  # per-vaccine columns below).
  parse_cdphe <- function(data_raw, id_cols) {
    data_base <- data_raw %>%
      filter(Metric %in% names(METRIC_KEYS)) %>%
      mutate(
        year_end = str_extract(Year_, "\\d{4}$"),
        time = as.Date(school_year_time_from_end(year_end)),
        grade = Survey_Type,
        vaccine_key = tolower(Vaccine),
        vaccine_key = gsub("[^a-z0-9]+", "_", vaccine_key),
        vaccine_key = gsub("_+", "_", vaccine_key),
        # CDPHE's raw label is "HepB" (no separator), which survives the regex
        # above as "hepb" -- align it to the "hep_b" spelling every other state
        # uses for this vaccine.
        vaccine_key = if_else(vaccine_key == "hepb", "hep_b", vaccine_key),
        metric_key = unname(METRIC_KEYS[Metric]),
        value = as.numeric(Value_Percent)
      ) %>%
      # Every metric kept above is published per vaccine, so a blank vaccine
      # here would be a parser failure rather than a real row.
      filter(!is.na(time), !is.na(vaccine_key), vaccine_key != "")

    keys <- c(id_cols, "time", "grade")

    data_wide <- data_base %>%
      mutate(col_name = if_else(
        metric_key == "coverage",
        paste0("pct_", vaccine_key),
        paste0("pct_", vaccine_key, "_", metric_key)
      )) %>%
      select(all_of(keys), col_name, value) %>%
      pivot_wider(
        names_from = col_name,
        values_from = value,
        # CDPHE duplicates the 2022/2023 Tdap rows for Larimer County, Poudre
        # R-1 and the statewide total: every metric appears twice, once with
        # the published figure and once with 0. max() keeps the figure. It
        # cannot produce -Inf here -- Value_Percent is populated on every row
        # of every table.
        values_fn = list(value = function(x) max(x, na.rm = TRUE))
      )

    # CDPHE reports enrolment PER VACCINE, not per geography-year-grade: in
    # Adams County 2021/2022 the Tdap rows carry 47,061 (the grades Tdap is
    # required in) while the K-12 vaccines carry 85,200. There is therefore no
    # single N_enrolled for a row, and folding the two together would mis-scale
    # one of them, so each vaccine's denominator is kept under its own name.
    #
    # It does NOT vary by metric, though -- CDPHE repeats the same denominator
    # on each metric row for a given geography/year/grade/vaccine (checked on
    # every table: no group carries more than one value) -- so distinct()
    # collapses the three metric rows to one and each vaccine gets a single
    # denominator column, shared by that vaccine's coverage and two exemption
    # columns.
    data_enroll <- data_base %>%
      distinct(across(all_of(keys)), vaccine_key, Enrollment) %>%
      mutate(col_name = paste0("N_", vaccine_key, "_enrolled")) %>%
      select(-vaccine_key) %>%
      pivot_wider(
        names_from = col_name,
        values_from = Enrollment,
        values_fn = list(Enrollment = function(x) max(x, na.rm = TRUE))
      )

    left_join(data_wide, data_enroll, by = keys)
  }

  # ---- County and statewide -> data.csv.gz ----
  county_raw <- read_layer(LAYERS$county$dest)
  state_raw <- read_layer(LAYERS$state$dest)

  # The live API returns county names in upper case ("EL PASO") and includes an
  # "Unknown" bucket for records with no county; join_county_fips() folds the
  # casing away and requires both to be declared rather than guessed at.
  county_out <- parse_cdphe(county_raw, "County") %>%
    rename(county = County) %>%
    # Title-case only so the "Unknown" bucket keeps a readable label; county
    # matching itself is case-insensitive.
    mutate(county = str_to_title(county)) %>%
    # "Unknown" is dropped rather than kept with geography = NA: the standard
    # output carries only FIPS-coded geographies, and these 12 rows (records
    # CDPHE could not assign to a county) are in neither a county nor the
    # statewide total. They remain in raw/ if the bucket is ever needed.
    join_county_fips(
      "CO",
      statewide = c("State Total", "State Totals", "Total"),
      drop = "^unknown$"
    ) %>%
    # join_county_fips() already emits the canonical geography_name, so the raw
    # `county` label it was resolved from is a duplicate of it.
    select(-county) %>%
    mutate(type = "county")

  # The statewide table has no geography column at all; every row is the
  # state total, so the state FIPS is stamped directly. Its vaccine set is
  # the county table's minus COVID, which is why the COVID columns are NA on
  # the state rows.
  state_out <- parse_cdphe(state_raw, character()) %>%
    mutate(geography = "08", geography_name = "Colorado", type = "state")

  data_out <- bind_rows(county_out, state_out) %>%
    order_columns(c("time", "grade", "geography", "geography_name", "type"))

  out <- write_standard(data_out, "Colorado", "./standard/data.csv.gz", from = "percent")
  update_latest_year(latest_school_year(out),
                     ids = c("cdphe_county_csv", "cdphe_statewide_csv"))

  # ---- Facilities -> data_schools.csv.gz ----
  #
  # The facility tables identify a facility by ID plus Site_Name, District_Name,
  # Facility_Type and County. Within a school year those five are consistent
  # (one name, one district, one county per ID), but ACROSS years the ID is
  # not a stable identity: 838 IDs carry more than one distinct name, some of
  # them renames (Legacy Options HS -> Legacy Options High School) and some
  # plainly different schools (ID 9486 is Life Skills Center of Colorado
  # Springs in the first table and Eastlake High School in the second), and
  # 42 IDs move county between years. The output therefore keeps each year's
  # published name, district, type and county with the row, and carries the ID
  # as school_id so that two facilities published under the same name in the
  # same district and year (Columbine Elementary Preschool and Wildflower
  # Preschool, both in Boulder County, with different enrolments) stay
  # distinct. school_id identifies a facility within a year, not across years.
  #
  # Casing is the one thing folded: CDPHE re-cases names between years
  # ("Genoa-Hugo C113" / "Genoa-hugo C113", "La Junta Jr/Sr High School" /
  # "La Junta Jr/sr High School"). Where an ID's spellings differ only in case,
  # the spelling from the latest year is used throughout. Real renames are not
  # touched.
  latest_spelling <- function(data, id_col, name_col) {
    canon <- data %>%
      distinct(across(all_of(c(id_col, name_col))), Year_) %>%
      mutate(.key = tolower(.data[[name_col]])) %>%
      group_by(across(all_of(id_col)), .key) %>%
      arrange(desc(Year_), .by_group = TRUE) %>%
      summarise(.canon = first(.data[[name_col]]), .groups = "drop")
    data %>%
      mutate(.key = tolower(.data[[name_col]])) %>%
      left_join(canon, by = c(id_col, ".key")) %>%
      mutate(!!name_col := .canon) %>%
      select(-.key, -.canon)
  }

  facility_raw <- bind_rows(
    read_layer(LAYERS$facility_2017_2022$dest) %>% mutate(.layer = "2017_2022"),
    read_layer(LAYERS$facility_2023_2025$dest) %>% mutate(.layer = "2023_2025")
  )
  # The two facility tables split the series by school year (2017-18 to
  # 2022-23, then 2023-24 on). A year in both would double every row of it,
  # so the split is checked rather than assumed.
  layer_years <- facility_raw %>% distinct(.layer, Year_)
  overlap <- intersect(layer_years$Year_[layer_years$.layer == "2017_2022"],
                       layer_years$Year_[layer_years$.layer == "2023_2025"])
  if (length(overlap)) {
    stop("CO: school year(s) ", paste(overlap, collapse = ", "),
         " appear in both facility tables", call. = FALSE)
  }
  facility_raw <- facility_raw %>%
    select(-.layer) %>%
    latest_spelling("ID", "Site_Name") %>%
    latest_spelling("ID", "District_Name")

  schools_out <- parse_cdphe(facility_raw,
                             c("ID", "Site_Name", "District_Name", "Facility_Type", "County")) %>%
    rename(school_id = ID, school_name = Site_Name, district = District_Name,
           school_type = Facility_Type, county = County) %>%
    # Five online schools (District "Education reEnvisioned") are published
    # with County "Unknown". They are real facilities, so they stay, with no
    # county FIPS.
    join_county_fips("CO", no_fips = "Unknown") %>%
    select(-county) %>%
    mutate(type = "school") %>%
    order_columns(c("time", "geography", "geography_name", "type", "school_id",
                    "school_name", "district", "school_type", "grade")) %>%
    arrange(time, grade, geography, district, school_name, school_id)

  dup_id <- schools_out %>% count(time, school_id, grade) %>% filter(n > 1)
  if (nrow(dup_id)) {
    stop("CO: ", nrow(dup_id), " duplicated (time, school_id, grade) key(s) in the facility rows, e.g. ",
         paste(head(dup_id$school_id, 5), collapse = ", "), call. = FALSE)
  }
  same_name <- schools_out %>% count(time, school_name, district, grade) %>% filter(n > 1)
  message(sprintf(
    "CO: %d school rows, school years %s to %s; %d (time, school_name, district, grade) key(s) are shared by two facilities with different school_id",
    nrow(schools_out), min(schools_out$time), max(schools_out$time), nrow(same_name)))

  schools <- write_standard(schools_out, "Colorado schools",
                            "./standard/data_schools.csv.gz", from = "percent")
  update_latest_year(
    latest_school_year(filter(schools, time %in% school_year_time(2017:2022))),
    ids = "cdphe_facility_2017_2022_csv")
  update_latest_year(latest_school_year(schools), ids = "cdphe_facility_2023_2025_csv")

  # ---- Districts -> data_districts.csv.gz ----
  #
  # The district table carries no county. The facility table does, and names
  # its districts identically (every district-table name appears there), so a
  # district whose facilities all sit in one county across every year takes
  # that county's FIPS. The 17 districts whose facilities span more than one
  # county (Charter School Institute is in 16) are left with geography NA:
  # they are real sub-state areas with no single county, the case
  # join_county_fips()'s no_fips= exists for, but with no label of their own
  # to pass through it.
  district_raw <- read_layer(LAYERS$district$dest) %>%
    latest_spelling("ID", "District_Name")

  district_county <- facility_raw %>%
    distinct(district_key = tolower(District_Name), County) %>%
    group_by(district_key) %>%
    summarise(county = if (n() == 1L) County[[1]] else NA_character_, .groups = "drop")

  districts_out <- parse_cdphe(district_raw, c("ID", "District_Name")) %>%
    rename(district_id = ID, district = District_Name) %>%
    mutate(district_key = tolower(district)) %>%
    left_join(district_county, by = "district_key") %>%
    select(-district_key)
  with_county <- districts_out %>%
    filter(!is.na(county)) %>%
    join_county_fips("CO", no_fips = "Unknown")
  without_county <- districts_out %>%
    filter(is.na(county)) %>%
    mutate(geography = NA_character_, geography_name = NA_character_)
  districts_out <- bind_rows(with_county, without_county) %>%
    select(-county) %>%
    mutate(type = "district") %>%
    order_columns(c("time", "geography", "geography_name", "type", "district_id",
                    "district", "grade")) %>%
    arrange(time, grade, district, district_id)

  dup_d <- districts_out %>% count(time, district_id, grade) %>% filter(n > 1)
  if (nrow(dup_d)) {
    stop("CO: ", nrow(dup_d), " duplicated (time, district_id, grade) key(s) in the district rows",
         call. = FALSE)
  }
  message(sprintf(
    "CO: %d district rows, %d with a county FIPS, %d districts spanning more than one county",
    nrow(districts_out), sum(!is.na(districts_out$geography)),
    n_distinct(districts_out$district[is.na(districts_out$geography)])))

  districts <- write_standard(districts_out, "Colorado districts",
                              "./standard/data_districts.csv.gz", from = "percent")
  update_latest_year(latest_school_year(districts), ids = "cdphe_district_csv")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
