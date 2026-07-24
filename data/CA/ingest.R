library(dcf)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(readxl)
library(vroom)
source("../../resources/add_state_column.R")

# =============================================================================
# CA - Kindergarten & 7th-grade School Immunizations (county-level)
#
# Hybrid, self-updating source strategy (no single multi-year county file exists):
#   * CHHS Open Data (school-level, Socrata) aggregated to county for the years
#     it covers (KG 2016-17..2022-23; 7th 2013-14..2019-20). School-level percents
#     are integer-rounded, so county aggregates are within ~1pp of CDPH's official
#     figures.
#   * CDPH published county report (Table 2) for the latest year (2024-25), which
#     provides official county aggregates directly.
# Output schema is unchanged: county-level KG (data_kg), 7th (data_7th), and the
# combined data.csv.gz.
# =============================================================================

# ---- Source URLs ----
chhs_kg_urls <- c(
  "raw/CA_KG_chhs_2016-17_to_2018-19.csv" =
    "https://data.chhs.ca.gov/dataset/bc38e725-9180-49e7-97e5-e16cb413a40c/resource/4319a7e8-5c63-460c-b412-c7474fd7da2a/download/iz_kindergarten2016-17_to_2018-19_school_year.csv",
  "raw/CA_KG_chhs_2019-20_to_2022-23.csv" =
    "https://data.chhs.ca.gov/dataset/bc38e725-9180-49e7-97e5-e16cb413a40c/resource/a269c0af-3fa7-4b27-8f5b-0bb0dcedfdd2/download/kindergarten_immunizations_academic_year_2019-20-to-2022-23.csv"
)
chhs_7th_urls <- c(
  "raw/CA_7th_chhs_tdap_vari_2016-17_to_2019-20.csv" =
    "https://data.chhs.ca.gov/dataset/240b6d05-3c72-4441-9420-93a2fea9b67c/resource/4376ad3f-6b96-46d5-8307-b227cfefa11b/download/odp_seventh_grade_tdap_vari_2016-17_to_2019-20.csv"
)
cdph_kg_url <- "https://www.cdph.ca.gov/Programs/CID/DCDC/CDPH%20Document%20Library/Immunization/2024-25KindergartenReport.xlsx"
cdph_kg_file <- "raw/CA_KG_cdph_2024-25.xlsx"

# ---- Download ----
dir.create("raw", showWarnings = FALSE)
download_quiet <- function(url, dest) {
  try(download.file(url, dest, mode = "wb", quiet = TRUE), silent = TRUE)
}
for (nm in names(chhs_kg_urls)) download_quiet(chhs_kg_urls[[nm]], nm)
for (nm in names(chhs_7th_urls)) download_quiet(chhs_7th_urls[[nm]], nm)
download_quiet(cdph_kg_url, cdph_kg_file)

# ---- Process guard ----
raw_files <- list.files("raw", recursive = TRUE, full.names = TRUE)
raw_state <- as.list(tools::md5sum(raw_files))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # ---- Helpers ----
  norm_names <- function(x) {
    x <- tolower(trimws(x))
    x <- gsub("[^a-z0-9]+", "_", x)
    gsub("^_|_$", "", x)
  }
  cat_key <- function(x) gsub("[^A-Z0-9]", "", toupper(x))
  num <- function(x) suppressWarnings(readr::parse_number(
    as.character(x), na = c("", "NA", "N/A", "N/A*", "*", "—", "–", "-")
  ))
  # school-year string -> Sept 1 of the start year
  sy_to_time <- function(x) {
    yr <- str_extract(as.character(x), "\\d{4}")
    as.Date(paste0(yr, "-09-01"))
  }
  # integer-percent files are 0-100; if a file stores proportions, rescale
  rescale_pct <- function(p) if (isTRUE(stats::median(p, na.rm = TRUE) <= 1.5)) p * 100 else p

  read_chhs <- function(path) {
    d <- readr::read_csv(path, show_col_types = FALSE, col_types = readr::cols(.default = "c"))
    names(d) <- norm_names(names(d))
    d
  }

  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  fips_ca <- all_fips %>%
    filter(state == "CA", nchar(geography) == 5) %>%
    mutate(join_name = str_to_title(gsub(" County$", "", geography_name))) %>%
    select(geography, geography_name, join_name)
  state_fips <- "06"

  # Attach county FIPS; unmatched (e.g. "State Total") -> statewide "Total"/06
  attach_fips <- function(df) {
    df %>%
      mutate(join_name = str_to_title(str_trim(county))) %>%
      left_join(fips_ca, by = "join_name") %>%
      mutate(
        geography = if_else(is.na(geography), state_fips, geography),
        geography_name = if_else(is.na(geography_name), "Total", geography_name)
      )
  }

  cats_kg <- c("UPTODATE", "CONDITIONAL", "PME", "OTHERS", "OVERDUE")
  kg_map <- c(
    UPTODATE = "pct_all_required", CONDITIONAL = "pct_conditional",
    PME = "pct_pme", OTHERS = "pct_other_lacking", OVERDUE = "pct_overdue"
  )

  # ---- KG: aggregate CHHS school-level -> county ----
  kg_chhs_files <- names(chhs_kg_urls)[file.exists(names(chhs_kg_urls))]
  kg_county_chhs <- lapply(kg_chhs_files, function(path) {
    d <- read_chhs(path) %>%
      transmute(
        school_year, school_code,
        county = toupper(str_trim(county)),
        enrollment = suppressWarnings(as.numeric(enrollment)),
        percent = suppressWarnings(as.numeric(percent)),
        ckey = cat_key(category)
      )
    d$percent <- rescale_pct(d$percent)

    enr <- d %>%
      distinct(school_year, county, school_code, enrollment) %>%
      group_by(school_year, county) %>%
      summarise(N_students = sum(enrollment, na.rm = TRUE), .groups = "drop")

    pcts <- d %>%
      filter(ckey %in% cats_kg, !is.na(percent), !is.na(enrollment)) %>%
      group_by(school_year, county, ckey) %>%
      summarise(pct = sum(percent * enrollment) / sum(enrollment), .groups = "drop") %>%
      pivot_wider(names_from = ckey, values_from = pct)

    enr %>% left_join(pcts, by = c("school_year", "county"))
  }) %>% bind_rows()

  # rename category columns -> schema
  present <- intersect(names(kg_map), names(kg_county_chhs))
  kg_county_chhs <- kg_county_chhs %>%
    rename_with(~ unname(kg_map[.x]), all_of(present)) %>%
    mutate(time = sy_to_time(school_year), grade = "Kindergarten") %>%
    attach_fips()

  # ---- KG: CDPH official county file (latest year) ----
  kg_cdph <- NULL
  if (file.exists(cdph_kg_file)) {
    kg_cdph <- tryCatch({
      raw <- readxl::read_excel(cdph_kg_file, sheet = "Table 2", skip = 2, col_names = FALSE)
      names(raw) <- paste0("V", seq_len(ncol(raw)))
      raw %>%
        transmute(
          county = str_trim(V1),
          N_students = num(V2),
          pct_all_required = num(V3) * 100,
          pct_conditional = num(V4) * 100,
          pct_pme = num(V5) * 100,
          pct_other_lacking = num(V6) * 100,
          pct_overdue = num(V7) * 100
        ) %>%
        filter(!is.na(county), county != "") %>%
        mutate(time = as.Date("2024-09-01"), grade = "Kindergarten") %>%
        attach_fips() %>%
        # keep only rows that matched a county or the state total
        filter(geography != state_fips | str_detect(tolower(county), "state|total|california"))
    }, error = function(e) NULL)
  }

  schema_kg <- c("geography", "geography_name", "time", "grade", "N_students",
                 "pct_all_required", "pct_conditional", "pct_pme",
                 "pct_other_lacking", "pct_overdue")
  ensure_cols <- function(df, cols) {
    for (c in setdiff(cols, names(df))) df[[c]] <- NA_real_
    df[, cols]
  }

  kg_out <- bind_rows(
    ensure_cols(kg_county_chhs, schema_kg),
    if (!is.null(kg_cdph)) ensure_cols(kg_cdph, schema_kg)
  ) %>%
    filter(!is.na(time)) %>%
    arrange(time, geography_name)

  # ---- 7th grade: aggregate CHHS school-level -> county (by vaccine) ----
  vax_map <- c(TDAP = "tdap", VARI = "varicella", VARICELLA = "varicella")
  g7_map <- c(
    UPTODATE = "pct_entrants_vax", CONDITIONAL = "pct_conditional",
    PME = "pct_pme", OTHERS = "pct_other_lacking", OVERDUE = "pct_overdue"
  )
  g7_files <- names(chhs_7th_urls)[file.exists(names(chhs_7th_urls))]
  g7_out <- NULL
  if (length(g7_files) > 0) {
    g7_long <- lapply(g7_files, function(path) {
      d <- read_chhs(path) %>%
        transmute(
          school_year, school_code,
          county = toupper(str_trim(county)),
          enrollment = suppressWarnings(as.numeric(enrollment)),
          percent = suppressWarnings(as.numeric(percent)),
          vax = recode(cat_key(vaccine), !!!as.list(vax_map), .default = NA_character_),
          ckey = cat_key(category)
        ) %>%
        # CHHS only publishes 7th-grade enrollment for 2019-20 onward; without
        # enrollment we cannot weight the county aggregate, so drop those rows.
        filter(!is.na(vax), !is.na(enrollment), enrollment > 0)
      d$percent <- rescale_pct(d$percent)

      enr <- d %>%
        distinct(school_year, county, vax, school_code, enrollment) %>%
        group_by(school_year, county, vax) %>%
        summarise(N_students = sum(enrollment, na.rm = TRUE), .groups = "drop")

      pcts <- d %>%
        filter(ckey %in% names(g7_map), !is.na(percent), !is.na(enrollment)) %>%
        group_by(school_year, county, vax, ckey) %>%
        summarise(pct = sum(percent * enrollment) / sum(enrollment), .groups = "drop") %>%
        mutate(ckey = unname(g7_map[ckey])) %>%
        pivot_wider(names_from = ckey, values_from = pct)

      enr %>% left_join(pcts, by = c("school_year", "county", "vax"))
    }) %>% bind_rows()

    g7_out <- g7_long %>%
      mutate(time = sy_to_time(school_year), grade = "7th") %>%
      attach_fips() %>%
      pivot_wider(
        id_cols = c(geography, geography_name, time, grade),
        names_from = vax,
        values_from = c(N_students, pct_entrants_vax, pct_conditional,
                        pct_pme, pct_other_lacking, pct_overdue),
        names_glue = "{.value}_{vax}"
      ) %>%
      filter(!is.na(time)) %>%
      arrange(time, geography_name)
  }

  # ---- Write outputs ----
  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(add_state_column(kg_out, "California"), "./standard/data_kg.csv.gz")
  if (!is.null(g7_out)) {
    vroom::vroom_write(add_state_column(g7_out, "California"), "./standard/data_7th.csv.gz")
  }

  data <- bind_rows(
    kg_out %>% mutate(source_grade = "KG"),
    if (!is.null(g7_out)) g7_out %>% mutate(source_grade = "7th")
  )
  vroom::vroom_write(add_state_column(data, "California"), "./standard/data.csv.gz")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
