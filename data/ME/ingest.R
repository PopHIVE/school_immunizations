# =============================================================================
# ME - School Vaccination Rates (Multiple Years)
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

raw_files <- list.files("raw", pattern = "School Vaccination Rates|School-Vaccination-Rates", full.names = TRUE)
raw_state <- list(hash = tools::md5sum(raw_files))

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

if (!identical(process$raw_state, raw_state)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "ME") %>%
    select(geography, geography_name, state)

  process_file <- function(path) {
    end_year <- parse_end_year(basename(path))
    time <- format(as.Date(paste0(end_year, "-12-31")), "%m-%d-%Y")

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

  data <- bind_rows(lapply(raw_files, process_file)) %>%
    mutate(
      county = str_to_title(str_trim(county)),
      geography_name = paste0(county, " County")
    ) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(state == "ME")

  pct_cols <- names(data)[grepl("^pct_", names(data))]
  data <- data %>%
    mutate(
      across(
        all_of(pct_cols),
        ~ if_else(!is.na(.x) & .x <= 1.5, .x * 100, .x)
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
  vroom::vroom_write(data_out, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
