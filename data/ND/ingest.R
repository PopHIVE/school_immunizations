source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
# =============================================================================
# ND - School Immunization Dashboard (School and County Level)
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

raw_file <- "raw/Dashboard data for requests.xlsx"
raw_state <- list(hash = tools::md5sum(raw_file))

script_hash <- as.character(tools::md5sum("ingest.R"))

# Gated on the script as well as the data, like every other state, so an
# edit to the parsing below is actually applied to standard/.
if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "ND") %>%
    # Bare county names, matching join_county_fips() in the other states.
    mutate(geography_name = sub(" County$", "", geography_name)) %>%
    select(geography, geography_name, state) %>%
    # Joined case-insensitively below: NDDoH's raw County spelling doesn't
    # round-trip through str_to_title() for compound names ("LaMoure" ->
    # "Lamoure", "McHenry" -> "Mchenry", ...), which silently dropped those
    # 5 counties. geography_name here is the FIPS-correct display spelling.
    mutate(join_key = tolower(geography_name))

  raw <- read_excel(raw_file, sheet = 1)

  percent_cols <- names(raw)[grepl("^%", names(raw))]

  # Explicit lookup on a punctuation-stripped key, rather than an ordered list
  # of str_replace_all() substitutions. In that list the generic "% " -> "pct_"
  # rule ran first and shadowed the specific ones, so NDDoH's "% ME" and "% RE"
  # came out as pct_me and pct_re -- never the pct_medical_exempt and
  # pct_religious_exempt the same map went on to ask for.
  #
  # The up-to-date coverage columns are mapped onto the plain vaccine names the
  # other 37 states use, so ND's MMR coverage is comparable without a per-state
  # alias. "Chickenpox" is NDDoH's name for varicella. PBE is a personal belief
  # exemption, reported here under the standard personal-exemption name.
  nd_measure <- c(
    me            = "pct_medical_exempt",
    re            = "pct_religious_exempt",
    pbe           = "pct_personal_exempt",
    norecord      = "pct_no_record",
    utdmmr        = "pct_mmr",
    utdpolio      = "pct_polio",
    utddtap       = "pct_dtap",
    utdhepb       = "pct_hep_b",
    utdchickenpox = "pct_varicella",
    utdtdap       = "pct_tdap",
    # MCV4 is the quadrivalent meningococcal conjugate vaccine, i.e. MenACWY --
    # the name MA, ME and ND's own measure_info.json already use for it.
    utdmcv4       = "pct_menacwy"
  )

  rename_pct_cols <- function(nms) {
    key <- gsub("[^a-z0-9]", "", tolower(nms))
    unknown <- nms[!key %in% names(nd_measure)]
    if (length(unknown)) {
      stop("ND: no standard name for column(s): ",
           paste(unknown, collapse = ", "), call. = FALSE)
    }
    unname(nd_measure[key])
  }

  data <- raw %>%
    mutate(
      join_key = tolower(str_trim(County)),
      school_name = str_to_title(str_trim(Schoolname)),
      grade = case_when(
        tolower(Grade) == "k" ~ "Kindergarten",
        TRUE ~ Grade
      ),
      end_year = as.integer(str_sub(`School Year`, -4, -1)),
      time = school_year_time_from_end(end_year)
    ) %>%
    left_join(county_fips_lookup, by = "join_key")

  # Report counties that failed the FIPS join instead of dropping them
  # silently, as MI does for its building-level county names.
  unmatched <- sort(unique(str_trim(data$County)[is.na(data$geography)]))
  if (length(unmatched)) {
    warning(length(unmatched), " county name(s) matched no ND FIPS and were dropped: ",
            paste(unmatched, collapse = ", "), call. = FALSE)
  }
  data <- data %>% filter(!is.na(geography), state == "ND")

  # One row per school x grade x school year, as NDDoH publishes it.
  schools <- data %>%
    rename_with(rename_pct_cols, all_of(percent_cols)) %>%
    mutate(type = "school", N_enrolled = Enrolled) %>%
    select(time, geography_name, geography, type, school_name, grade,
           N_enrolled, starts_with("pct_"))

  # County rows, derived by aggregating the school rows above -- NDDoH
  # publishes only percentages (no underlying counts to sum), so each county
  # rate is an enrollment-weighted mean of its schools' rates rather than an
  # exact recomputation from summed counts.
  counties <- data %>%
    group_by(geography, geography_name, time, grade) %>%
    summarize(
      N_enrolled = sum(Enrolled, na.rm = TRUE),
      across(
        all_of(percent_cols),
        ~ if (all(is.na(.x))) NA_real_ else weighted.mean(.x, Enrolled, na.rm = TRUE)
      ),
      .groups = "drop"
    ) %>%
    rename_with(rename_pct_cols, all_of(percent_cols)) %>%
    mutate(type = "county", school_name = NA_character_) %>%
    select(time, geography_name, geography, type, school_name, grade,
           N_enrolled, starts_with("pct_"))

  data_out <- bind_rows(schools, counties)

  message(sprintf(
    "ND: %d rows (%d school, %d county), school years %s",
    nrow(data_out), sum(data_out$type == "school"), sum(data_out$type == "county"),
    paste(sort(unique(data_out$time)), collapse = ", ")))

  dir.create("standard", showWarnings = FALSE)
  write_standard(data_out, "North Dakota", "standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
