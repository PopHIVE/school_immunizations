library(dcf)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)
library(vroom)
library(readr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/county_fips.R")

raw_state <- as.list(tools::md5sum(list.files(
  "raw", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  # Each raw workbook has one sheet per vaccine -- Polio, DTP DTaP TD, Tdap,
  # Measles, Mumps, Rubella, Hepatitis B, HIb (a few files misspell it
  # "Hlb"), Varicella Chicken Pox, Pneumococcal, Meningococcal -- plus a
  # "Summary" sheet (sheet-level row/column counts, not data) and, in the
  # earliest files, a one-row "City of Chicago SD 299" district breakdown
  # that duplicates part of Cook County at a different granularity. Only the
  # per-vaccine sheets are county-level data on this file's key, so only
  # those are read; every vaccine's own measures are kept in its own
  # columns (rate_<vax>, rate_<vax>_religious_exempt, ...) rather than
  # collapsed into one arbitrary vaccine's numbers, which is what reading
  # only the first sheet used to do -- silently, and inconsistently between
  # files, since sheet order is not the same from year to year.
  VAX_MAP <- c(
    "Polio" = "polio",
    "DTP DTaP TD" = "dtap",
    "Tdap" = "tdap",
    "Measles" = "measles",
    "Mumps" = "mumps",
    "Rubella" = "rubella",
    "Hepatitis B" = "hep_b",
    "HIb" = "hib",
    "Hlb" = "hib",
    "Varicella Chicken Pox" = "varicella",
    "Pneumococcal" = "pcv",
    "Meningococcal" = "menacwy"
  )
  NON_VAX_SHEETS <- c("Summary", "City of Chicago SD 299")

  raw_files <- list.files("./raw", pattern = "\\.xlsx$", full.names = TRUE)

  year_end_from_path <- function(path) {
    year_pair <- str_match(basename(path), "_(\\d{4})_(\\d{2})")
    year_single <- str_match(basename(path), "_(\\d{4})")
    if (!is.na(year_pair[2]) && !is.na(year_pair[3])) {
      paste0(substr(year_pair[2], 1, 2), year_pair[3])
    } else if (!is.na(year_single[2])) {
      year_single[2]
    } else {
      NA_character_
    }
  }

  parse_sheet <- function(path, sheet, vax, time) {
    raw <- readxl::read_excel(path, sheet = sheet)
    # Collapse repeated whitespace in the header before referring to it by
    # name: "Unduplicated Count  Non-compliant-Immunization Requirements" has
    # two spaces before "Non-compliant" in most files but one in the 2023 and
    # 2025 workbooks, the same kind of between-file inconsistency as the
    # "HIb"/"Hlb" sheet-name typo.
    names(raw) <- gsub("\\s+", " ", trimws(names(raw)))
    raw %>%
      transmute(
        county = str_trim(County),
        vax = vax,
        enrollment = readr::parse_number(as.character(`Enrollment PreK-12`)),
        non_compliant = readr::parse_number(as.character(
          `Unduplicated Count Non-compliant-Immunization Requirements`)),
        protected = readr::parse_number(as.character(`Protected and in compliance`)),
        religious = readr::parse_number(as.character(`Religious objection`)),
        medical = readr::parse_number(as.character(`Medical reasons`)),
        school_compliance = readr::parse_number(as.character(`School Compliance %`)),
        time = time
      ) %>%
      filter(!is.na(time), !is.na(county), county != "")
  }

  parse_one <- function(path) {
    year_end <- year_end_from_path(path)
    time <- if (!is.na(year_end)) as.Date(school_year_time_from_end(year_end)) else as.Date(NA)

    sheets <- readxl::excel_sheets(path)
    unmapped <- setdiff(sheets, c(names(VAX_MAP), NON_VAX_SHEETS))
    if (length(unmapped)) {
      stop("IL: unmapped sheet(s) in ", basename(path), ": ",
           paste(unmapped, collapse = ", "), call. = FALSE)
    }
    vax_sheets <- intersect(sheets, names(VAX_MAP))
    bind_rows(lapply(vax_sheets, function(s) {
      parse_sheet(path, s, VAX_MAP[[s]], time)
    }))
  }

  data_long <- bind_rows(lapply(raw_files, parse_one))

  # Most measures are emitted per vaccine, in the column name:
  # N_<vax>_religious_exempt, N_<vax>_medical_exempt, N_<vax>_non_compliant,
  # rate_<vax> (coverage), rate_<vax>_religious_exempt,
  # rate_<vax>_medical_exempt, rate_<vax>_non_compliant. There is no
  # per-vaccine coverage COUNT (N_<vax>) -- only the rate -- and enrollment is
  # collapsed to a single shared N_enrolled rather than kept per vaccine,
  # even though it is not actually constant across vaccines: Tdap and
  # Meningococcal assess a 7th/9-12 grade cohort while Polio/DTaP/Measles
  # assess PreK-12. N_enrolled below takes the largest figure across a
  # county/year's vaccine sheets -- the full PreK-12 population -- so
  # Tdap/Meningococcal's smaller cohort undercounts against it; each
  # vaccine's own rate_<vax>* is still computed from that vaccine's own
  # correct enrollment via rate_from_counts(), so only the standalone
  # N_enrolled column, not the rates, is approximate for those two vaccines.
  counts_long <- data_long %>%
    transmute(
      county, time, vax,
      N_religious_exempt = religious,
      N_medical_exempt = medical,
      N_non_compliant = non_compliant
    ) %>%
    pivot_longer(starts_with("N_"), names_to = "measure", values_to = "value") %>%
    mutate(column = paste0("N_", vax, "_", sub("^N_", "", measure)))

  enrolled_long <- data_long %>%
    group_by(county, time) %>%
    summarise(value = suppressWarnings(max(enrollment, na.rm = TRUE)), .groups = "drop") %>%
    mutate(value = ifelse(is.infinite(value), NA_real_, value), column = "N_enrolled")

  rates_raw <- data_long %>%
    transmute(
      county, time, vax,
      rate = rate_from_counts(protected, enrollment),
      rate_religious_exempt = rate_from_counts(religious, enrollment),
      rate_medical_exempt = rate_from_counts(medical, enrollment),
      rate_non_compliant = rate_from_counts(non_compliant, enrollment)
    )

  # A share above 1 is not a measurement: the source's own numerator exceeds
  # its own enrollment for that vaccine -- e.g. Cook County's 2025 Hepatitis B
  # sheet reports 817,215 "Protected and in compliance" against 739,358
  # enrolled, and its own "Compliance % (weighted-by-count)" prints the same
  # impossible 110.5%. Rather than publish a rate above 1, it is dropped to
  # NA and logged, the same rule CT applies to source percentages outside
  # [-5, 105]. The counts themselves are left as the source printed them --
  # only the derived rate is suppressed.
  clamp_rate <- function(x) ifelse(!is.na(x) & x > 1, NA_real_, x)
  rate_cols <- c("rate", "rate_religious_exempt", "rate_medical_exempt",
                 "rate_non_compliant")
  impossible <- Reduce(`|`, lapply(rate_cols, function(cc) {
    v <- rates_raw[[cc]]
    !is.na(v) & v > 1
  }))
  if (any(impossible)) {
    bad <- rates_raw[impossible, c("county", "time", "vax")]
    message(sprintf(
      paste0("Illinois: dropping %d rate(s) above 1 -- the source's own ",
             "numerator exceeds its enrollment for that vaccine:\n  %s"),
      sum(impossible),
      paste(sprintf("%s %s %s", bad$county, bad$time, bad$vax), collapse = "\n  ")))
  }
  for (cc in rate_cols) rates_raw[[cc]] <- clamp_rate(rates_raw[[cc]])

  rates_long <- rates_raw %>%
    pivot_longer(starts_with("rate"), names_to = "measure", values_to = "value") %>%
    mutate(column = if_else(
      measure == "rate",
      paste0("rate_", vax),
      paste0("rate_", vax, "_", sub("^rate_", "", measure))
    ))

  # Illinois's own "School Compliance %" is kept under its own name rather
  # than folded into rate_<vax>_non_compliant: the two do not reconcile
  # arithmetically (Adams County Polio 2024: 99.96% school compliance against
  # a non-compliant headcount of 12 out of 10,513 enrolled, which is 99.89%),
  # so they are evidently not the same computation and neither can stand in
  # for the other. It is on a 0-100 scale, so it gets the pct_ prefix write_
  # standard() converts, rather than rate_from_counts()'s already-0-1 scale.
  pct_long <- data_long %>%
    transmute(county, time, vax, value = school_compliance) %>%
    mutate(column = paste0("pct_", vax, "_school_compliance"))

  data_wide <- bind_rows(counts_long, rates_long, pct_long, enrolled_long) %>%
    select(county, time, column, value) %>%
    pivot_wider(id_cols = c(county, time), names_from = column, values_from = value)

  # The hand-written county_join fixups for DeWitt/La Salle/Saint Clair are
  # gone: join_county_fips() matches those on a normalized key.
  data_out <- data_wide %>%
    join_county_fips("IL") %>%
    mutate(grade = "Overall") %>%
    select(-county) %>%
    select(time, geography, geography_name, grade, everything())

  # Every pct_ column here is the school-compliance figure above, published
  # on a 0-100 scale; write_standard() converts it to rate_<vax>_school_
  # compliance on the standard 0-1 scale and leaves the already-computed
  # rate_ columns untouched.
  write_standard(data_out, "Illinois", "./standard/data.csv.gz", from = "percent")

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
