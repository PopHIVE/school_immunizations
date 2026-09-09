source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/fetch.R")
# =============================================================================
# ME - School Vaccination Rates (Multiple Years)
# =============================================================================

library(dplyr)
library(readxl)
library(stringr)
library(vroom)

sources <- read_sources()
src <- source_entry(sources, "mecdc_workbooks")
process <- dcf::dcf_process_record()
prev <- process$fetch_state

# ---- Download School Vaccination Rates workbooks from Maine CDC ----
# The immunization data-reports page links a workbook per school year, but the
# hrefs are not all live: the 2020-2021 and 2022-2023 links point at paths
# under data-reports/ that 404, while every workbook, those included, is served
# from the immunization-reports directory under its basename. So the listing
# supplies the file names and each one is fetched from that directory. A year's
# workbook does not change once posted, so files already in raw/ are skipped;
# a listing that cannot be fetched is a warning and the run continues on raw/.
# fetch.R sends the browser header set maine.gov requires (it 403s bare
# clients).
dir.create("raw", showWarnings = FALSE)
me_base <- "https://www.maine.gov/dhhs/mecdc/sites/maine.gov.dhhs.mecdc/files/immunization-reports/"
links <- discover_links(src$page_url, src$pattern, must_find = FALSE)
me_files <- unique(utils::URLdecode(basename(links$href)))
recs <- fetch_many(
  paste0(me_base, utils::URLencode(me_files, reserved = FALSE)),
  dests = file.path("raw", me_files),
  if_exists = "skip", previous = prev
)
process <- record_fetch(process, recs)

raw_files <- list.files("raw", pattern = "School Vaccination Rates|School-Vaccination-Rates", full.names = TRUE)
raw_state <- raw_state_md5()

parse_end_year <- function(filename) {
  # Handles YYYY-YYYY and YYYY-YY patterns
  m <- str_match(filename, "(\\d{4})[-_](\\d{2,4})")
  if (is.na(m[1, 1])) return(NA_integer_)
  end <- m[1, 3]
  if (nchar(end) == 2) {
    end <- paste0("20", end)
  }
  as.integer(end)
}

script_hash <- as.character(tools::md5sum("ingest.R"))

# Gated on the script as well as the data, like every other state, so an edit
# to the parsing below is actually applied to standard/.
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "ME") %>%
    # Bare county names, matching join_county_fips() in the other states.
    mutate(geography_name = sub(" County$", "", geography_name)) %>%
    select(geography, geography_name, state)

  process_file <- function(path) {
    end_year <- parse_end_year(basename(path))
    time <- school_year_time_from_end(end_year)

    d <- read_excel(path, skip = 4)
    d <- d %>% filter(!is.na(School), !is.na(County))

    col_num <- function(name) {
      if (name %in% names(d)) {
        return(suppressWarnings(as.numeric(d[[name]])))
      }
      rep(NA_real_, nrow(d))
    }

    k <- d %>%
      transmute(
        school_name = School,
        county = County,
        grade = "Kindergarten",
        n_assessed = if ("Num Assessed...3" %in% names(d)) col_num("Num Assessed...3") else col_num("Assessed...3"),
        pct_exempt_total = col_num("TotalExempt...4"),
        pct_exempt_medical = col_num("Medical...5"),
        pct_exempt_religious = col_num("Religious...6"),
        pct_exempt_philosophical = col_num("Philosophical...7"),
        pct_90_day = col_num("90 Day...8"),
        pct_missing = if ("Missing...8" %in% names(d)) col_num("Missing...8") else col_num("Missing...9"),
        pct_dtap = col_num("4DTaP"),
        pct_polio = if ("3Polio" %in% names(d)) col_num("3Polio") else col_num("3Polio...11"),
        pct_mmr = if ("2MMR" %in% names(d)) col_num("2MMR") else col_num("2MMR...12"),
        pct_varicella = if ("1Varicella/Hx" %in% names(d)) col_num("1Varicella/Hx") else col_num("2Var...13"),
        pct_tdap = NA_real_,
        pct_menacwy = NA_real_,
        time = time
      )

    seventh <- d %>%
      transmute(
        school_name = School,
        county = County,
        grade = "7th Grade",
        n_assessed = if ("Num Assessed...13" %in% names(d)) col_num("Num Assessed...13") else col_num("Assessed...14"),
        pct_exempt_total = if ("TotalExempt...14" %in% names(d)) col_num("TotalExempt...14") else col_num("TotalExempt...15"),
        pct_exempt_medical = if ("Medical...15" %in% names(d)) col_num("Medical...15") else col_num("Medical...16"),
        pct_exempt_religious = if ("Religious...16" %in% names(d)) col_num("Religious...16") else col_num("Religious...17"),
        pct_exempt_philosophical = if ("Philosophical...17" %in% names(d)) col_num("Philosophical...17") else col_num("Philosophical...18"),
        pct_90_day = col_num("90 Day...19"),
        pct_missing = if ("Missing...18" %in% names(d)) col_num("Missing...18") else col_num("Missing...20"),
        pct_dtap = NA_real_,
        pct_polio = col_num("3Polio...22"),
        pct_mmr = col_num("2MMR...23"),
        pct_varicella = col_num("2Var...24"),
        pct_tdap = if ("1Tdap" %in% names(d)) col_num("1Tdap") else col_num("1Tdap...21"),
        pct_menacwy = if ("1MenACWY...20" %in% names(d)) col_num("1MenACWY...20") else col_num("1MenACWY"),
        time = time
      )

    twelfth <- d %>%
      transmute(
        school_name = School,
        county = County,
        grade = "12th Grade",
        n_assessed = if ("Num Assessed...21" %in% names(d)) col_num("Num Assessed...21") else col_num("Assessed...26"),
        pct_exempt_total = if ("TotalExempt...22" %in% names(d)) col_num("TotalExempt...22") else col_num("TotalExempt...27"),
        pct_exempt_medical = if ("Medical...23" %in% names(d)) col_num("Medical...23") else col_num("Medical...28"),
        pct_exempt_religious = if ("Religious...24" %in% names(d)) col_num("Religious...24") else col_num("Religious...29"),
        pct_exempt_philosophical = if ("Philosophical...25" %in% names(d)) col_num("Philosophical...25") else col_num("Philosophical...30"),
        pct_90_day = col_num("90 Day...31"),
        pct_missing = if ("Missing...26" %in% names(d)) col_num("Missing...26") else col_num("Missing...32"),
        pct_dtap = NA_real_,
        pct_polio = col_num("3Polio...34"),
        pct_mmr = col_num("2MMR...35"),
        pct_varicella = col_num("2Var...36"),
        pct_tdap = col_num("1Tdap...33"),
        pct_menacwy = if ("1MenACWY...27" %in% names(d)) col_num("1MenACWY...27") else col_num("2MenACWY"),
        time = time
      )

    bind_rows(k, seventh, twelfth)
  }

  # De-duplicate by school year, preferring freshly downloaded files (names
  # without a " (1)" suffix) over any older manual copies of the same year.
  raw_files <- raw_files[order(str_detect(basename(raw_files), "\\(1\\)"))]
  ey <- vapply(raw_files, function(p) parse_end_year(basename(p)), integer(1))
  raw_files <- raw_files[!is.na(ey) & !duplicated(ey)]

  data <- bind_rows(lapply(raw_files, process_file)) %>%
    mutate(
      county = str_to_title(str_trim(county)),
      geography_name = county
    ) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(state == "ME")

  # Maine CDC publishes percent points, so that is declared. The old
  # `if_else(.x <= 1.5, .x * 100, .x)` rescaled per element, turning a school
  # genuinely reporting 1.2% into 120% while leaving 12% alone -- two scales in
  # one column. Out-of-range values are dropped in both directions.
  pct_cols <- names(data)[grepl("^pct_", names(data))]
  data <- data %>%
    mutate(
      across(
        all_of(pct_cols),
        ~ if_else(!is.na(.x) & (.x < 0 | .x > 100), NA_real_, .x)
      )
    )

  data_out <- data %>%
    select(
      time,
      geography,
      geography_name,
      school_name,
      grade,
      n_assessed,
      starts_with("pct_")
    )

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(data_out, "Maine", "standard/data.csv.gz", from = "percent")
  update_latest_year(latest_school_year(out))

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
