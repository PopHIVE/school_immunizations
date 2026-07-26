# =============================================================================
# generate_measure_info.R
#
# Generates a PopHIVE-conforming measure_info.json for each ingested state from
# (1) a canonical measure dictionary shared across states and (2) a per-state
# source registry. For every state that has standard/data.csv.gz, the script
# reads the header, keeps the columns that are measures (skipping index/
# dimension columns), and writes one metadata object per measure plus a
# "_sources" block describing the state's data origin.
#
# Conventions: https://github.com/PopHIVE/Ingest/blob/main/.claude/skills/ingest-source.md
#
# Uses base R only (no jsonlite/vroom) so it runs both locally and in CI without
# adding dependencies. Run from the repo root:  Rscript scripts/generate_measure_info.R
# Optionally pass state abbreviations to restrict:  Rscript scripts/generate_measure_info.R OR CT
# =============================================================================

# ---- minimal JSON writer -----------------------------------------------------
# Handles: length-1 character (string), character vectors and unnamed lists
# (arrays), named lists (objects), and empty list() (-> []). Numbers are written
# as strings upstream, so no numeric branch is needed.

.esc <- function(s) {
  s <- gsub("\\", "\\\\", s, fixed = TRUE)
  s <- gsub("\"", "\\\"", s, fixed = TRUE)
  s <- gsub("\n", "\\n", s, fixed = TRUE)
  s <- gsub("\t", "\\t", s, fixed = TRUE)
  s
}

emit_json <- function(x, ind = 0L) {
  pad <- strrep("  ", ind)
  pad2 <- strrep("  ", ind + 1L)
  if (is.list(x)) {
    if (length(x) == 0L) return("[]")
    nm <- names(x)
    if (is.null(nm) || all(nm == "")) {
      items <- vapply(x, function(el) paste0(pad2, emit_json(el, ind + 1L)), "")
      return(paste0("[\n", paste(items, collapse = ",\n"), "\n", pad, "]"))
    }
    items <- vapply(seq_along(x), function(i) {
      paste0(pad2, "\"", .esc(nm[i]), "\": ", emit_json(x[[i]], ind + 1L))
    }, "")
    return(paste0("{\n", paste(items, collapse = ",\n"), "\n", pad, "}"))
  }
  if (is.character(x)) {
    if (length(x) == 0L) return("[]")
    if (length(x) == 1L) return(paste0("\"", .esc(x), "\""))
    items <- vapply(x, function(el) paste0(pad2, "\"", .esc(el), "\""), "")
    return(paste0("[\n", paste(items, collapse = ",\n"), "\n", pad, "]"))
  }
  stop("emit_json: unsupported type ", class(x)[1])
}

# ---- canonical measure dictionary --------------------------------------------
# One entry per measure column. `id` and `sources` are filled in per state at
# build time. Fields follow the PopHIVE measure_info schema.

m_coverage <- function(short, long_antigen, statement_antigen) {
  list(
    short_name = paste0(short, " coverage"),
    long_name = paste0("Percent of students up to date for ", long_antigen, " vaccination"),
    category = "immunization",
    short_description = paste0(
      "Percentage of assessed school students who are up to date for ",
      long_antigen, " vaccination."),
    long_description = paste0(
      "Share of assessed school students reported as up to date for ",
      long_antigen, " vaccination in the state's annual school immunization ",
      "survey. The antigen-specific series definition, the grade(s) assessed, ",
      "and whether the figure reflects all enrolled students or only those ",
      "surveyed vary by state; see the source description."),
    statement = paste0("In {location}, {value}% of school students were up to date for ",
      statement_antigen, " vaccination."),
    measure_type = "Percent",
    unit = "Percent",
    time_resolution = "Year"
  )
}

m_count <- function(short, long_antigen) {
  list(
    short_name = paste0(short, " students vaccinated"),
    long_name = paste0("Number of students up to date for ", long_antigen, " vaccination"),
    category = "immunization",
    short_description = paste0(
      "Number of assessed school students up to date for ", long_antigen,
      " vaccination."),
    long_description = paste0(
      "Count of assessed school students reported as up to date for ",
      long_antigen, " vaccination. Reported by some states and left missing ",
      "where the source publishes only percentages."),
    statement = paste0("In {location}, {value} school students were up to date for ",
      long_antigen, " vaccination."),
    measure_type = "Count",
    unit = "Students",
    time_resolution = "Year"
  )
}

MEASURES <- list(
  # antigen coverage (percent)
  pct_dtap = m_coverage("DTaP/Tdap", "diphtheria-tetanus-pertussis (DTaP/Tdap)", "DTaP/Tdap"),
  pct_polio = m_coverage("Polio", "polio (IPV/OPV)", "polio"),
  pct_mmr = m_coverage("MMR", "measles-mumps-rubella (MMR)", "MMR"),
  pct_hep_b = m_coverage("Hepatitis B", "hepatitis B", "hepatitis B"),
  pct_varicella = m_coverage("Varicella", "varicella (chickenpox)", "varicella"),

  # antigen coverage (counts)
  N_dtap = m_count("DTaP/Tdap", "diphtheria-tetanus-pertussis (DTaP/Tdap)"),
  N_polio = m_count("Polio", "polio (IPV/OPV)"),
  N_mmr = m_count("MMR", "measles-mumps-rubella (MMR)"),
  N_hep_b = m_count("Hepatitis B", "hepatitis B"),
  N_varicella = m_count("Varicella", "varicella (chickenpox)"),

  # exemptions (percent)
  pct_personal_exempt = list(
    short_name = "Non-medical exemption rate",
    long_name = "Percent of students with a non-medical (personal-belief) vaccination exemption",
    category = "immunization",
    short_description = "Percentage of assessed school students with a non-medical exemption (religious, philosophical, or personal-belief) from vaccination requirements.",
    long_description = "Share of assessed school students holding a non-medical exemption from one or more required vaccines. The exemption categories permitted differ by state law - some allow religious exemptions only, others also allow philosophical or personal-belief exemptions - and this harmonized field aggregates whichever non-medical categories the state reports.",
    statement = "In {location}, {value}% of school students had a non-medical vaccination exemption.",
    measure_type = "Percent",
    unit = "Percent",
    time_resolution = "Year"
  ),
  pct_medical_exempt = list(
    short_name = "Medical exemption rate",
    long_name = "Percent of students with a medical vaccination exemption",
    category = "immunization",
    short_description = "Percentage of assessed school students with a medical exemption from one or more required vaccines.",
    long_description = "Share of assessed school students with a physician-certified medical exemption from one or more required vaccines, as reported in the state's annual school immunization survey.",
    statement = "In {location}, {value}% of school students had a medical vaccination exemption.",
    measure_type = "Percent",
    unit = "Percent",
    time_resolution = "Year"
  ),
  pct_full_exempt = list(
    short_name = "Total exemption rate",
    long_name = "Percent of students with any vaccination exemption",
    category = "immunization",
    short_description = "Percentage of assessed school students with any exemption (medical or non-medical) from vaccination requirements.",
    long_description = "Share of assessed school students with at least one vaccination exemption of any type, combining medical and non-medical exemptions. Where a state reports the categories separately this is their sum; overlap handling depends on what the source publishes.",
    statement = "In {location}, {value}% of school students had a vaccination exemption of any kind.",
    measure_type = "Percent",
    unit = "Percent",
    time_resolution = "Year"
  ),

  # exemptions (counts)
  N_personal_exempt = list(
    short_name = "Non-medical exemptions (count)",
    long_name = "Number of students with a non-medical (personal-belief) vaccination exemption",
    category = "immunization",
    short_description = "Number of assessed school students with a non-medical exemption from vaccination requirements.",
    long_description = "Count of assessed school students holding a non-medical (religious, philosophical, or personal-belief) exemption from one or more required vaccines. Reported by some states and left missing where the source publishes only percentages.",
    statement = "In {location}, {value} school students had a non-medical vaccination exemption.",
    measure_type = "Count",
    unit = "Students",
    time_resolution = "Year"
  ),
  N_medical_exempt = list(
    short_name = "Medical exemptions (count)",
    long_name = "Number of students with a medical vaccination exemption",
    category = "immunization",
    short_description = "Number of assessed school students with a medical exemption from vaccination requirements.",
    long_description = "Count of assessed school students with a physician-certified medical exemption from one or more required vaccines. Reported by some states and left missing where the source publishes only percentages.",
    statement = "In {location}, {value} school students had a medical vaccination exemption.",
    measure_type = "Count",
    unit = "Students",
    time_resolution = "Year"
  ),
  N_full_exempt = list(
    short_name = "Total exemptions (count)",
    long_name = "Number of students with any vaccination exemption",
    category = "immunization",
    short_description = "Number of assessed school students with any exemption (medical or non-medical) from vaccination requirements.",
    long_description = "Count of assessed school students with at least one vaccination exemption of any type, combining medical and non-medical exemptions. Reported by some states and left missing where the source publishes only percentages.",
    statement = "In {location}, {value} school students had a vaccination exemption of any kind.",
    measure_type = "Count",
    unit = "Students",
    time_resolution = "Year"
  )
)

# ---- extended dictionary: additional antigens, denominators, status, --------
# ---- antigen-specific and state-specific exemption categories ---------------

# additional single antigens (some states report these beyond the core five)
MEASURES$pct_tdap <- m_coverage("Tdap", "tetanus-diphtheria-pertussis (Tdap)", "Tdap")
MEASURES$pct_hep_a <- m_coverage("Hepatitis A", "hepatitis A", "hepatitis A")
MEASURES$pct_mcv4 <- m_coverage("Meningococcal (MCV4)", "meningococcal conjugate (MenACWY/MCV4)", "meningococcal (MenACWY)")
MEASURES$pct_menacwy <- m_coverage("Meningococcal (MenACWY)", "meningococcal conjugate (MenACWY)", "meningococcal (MenACWY)")
MEASURES$pct_hib_utd <- m_coverage("Hib", "Haemophilus influenzae type b (Hib)", "Hib")
MEASURES$pct_pcv_utd <- m_coverage("Pneumococcal (PCV)", "pneumococcal conjugate (PCV)", "pneumococcal (PCV)")
MEASURES$N_tdap <- m_count("Tdap", "tetanus-diphtheria-pertussis (Tdap)")
MEASURES$N_menacwy <- m_count("MenACWY", "meningococcal conjugate (MenACWY)")
MEASURES$N_hep_a <- m_count("Hepatitis A", "hepatitis A")

# enrollment / denominator counts (published under several column names)
m_denom <- function(short_name, long_name, short_desc, long_desc, statement) {
  list(short_name = short_name, long_name = long_name, category = "immunization",
       short_description = short_desc, long_description = long_desc,
       statement = statement, measure_type = "Count", unit = "Students",
       time_resolution = "Year")
}
enroll_entry <- m_denom(
  "Students enrolled", "Number of students enrolled",
  "Number of students enrolled at the reporting level.",
  "Enrollment count reported by the source. Used as the denominator for the coverage and exemption percentages in the same record.",
  "In {location}, {value} students were enrolled.")
MEASURES$N_enrolled <- enroll_entry
MEASURES$N_enroll <- enroll_entry
MEASURES$total_enrolled <- enroll_entry
MEASURES$enrollment <- enroll_entry
MEASURES$N_surveyed <- m_denom(
  "Students surveyed", "Number of students surveyed",
  "Number of students included in the immunization survey.",
  "Count of students captured by the school immunization survey, which may be smaller than total enrollment when reporting is incomplete.",
  "In {location}, {value} students were surveyed.")
MEASURES$n_assessed <- m_denom(
  "Students assessed", "Number of students assessed",
  "Number of students assessed for immunization status.",
  "Count of students whose immunization status was assessed at the reporting level; the denominator for that record's percentages.",
  "In {location}, {value} students were assessed.")
MEASURES$N_students <- m_denom(
  "Students assessed", "Number of students assessed",
  "Number of students (entrants) assessed for immunization status.",
  "Count of students (school entrants) assessed for immunization status; the enrollment-based denominator for the accompanying percentages.",
  "In {location}, {value} students were assessed.")

# immunization / enrollment status categories
MEASURES$pct_complete <- list(
  short_name = "Up-to-date rate",
  long_name = "Percent of students up to date for all required vaccines",
  category = "immunization",
  short_description = "Percentage of assessed school students up to date for all required vaccines.",
  long_description = "Share of assessed school students who have completed all age-appropriate required immunizations (fully immunized / up to date at the time of the survey).",
  statement = "In {location}, {value}% of school students were up to date for all required vaccines.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$N_complete <- list(
  short_name = "Up-to-date students (count)",
  long_name = "Number of students up to date for all required vaccines",
  category = "immunization",
  short_description = "Number of assessed school students up to date for all required vaccines.",
  long_description = "Count of assessed school students who have completed all age-appropriate required immunizations.",
  statement = "In {location}, {value} school students were up to date for all required vaccines.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$pct_all_required <- list(
  short_name = "All-required coverage",
  long_name = "Percent of students with all required immunizations",
  category = "immunization",
  short_description = "Percentage of students reported as having all required immunizations at school entry.",
  long_description = "School-entry assessment category (e.g. California kindergarten reporting): share of students up to date with all required immunizations at entry.",
  statement = "In {location}, {value}% of students had all required immunizations.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_conditional <- list(
  short_name = "Conditional enrollment rate",
  long_name = "Percent of students conditionally enrolled",
  category = "immunization",
  short_description = "Percentage of assessed school students admitted conditionally while completing required immunizations.",
  long_description = "Share of assessed school students who are conditionally (or provisionally) enrolled - permitted to attend while in the process of completing required immunizations and not yet overdue.",
  statement = "In {location}, {value}% of school students were conditionally enrolled.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_provisional <- list(
  short_name = "Provisional enrollment rate",
  long_name = "Percent of students provisionally enrolled",
  category = "immunization",
  short_description = "Percentage of assessed school students provisionally enrolled while completing required immunizations.",
  long_description = "Share of assessed school students permitted to attend on a provisional basis while completing required immunizations.",
  statement = "In {location}, {value}% of school students were provisionally enrolled.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$N_provisional <- list(
  short_name = "Provisional students (count)",
  long_name = "Number of students provisionally enrolled",
  category = "immunization",
  short_description = "Number of assessed school students provisionally enrolled while completing required immunizations.",
  long_description = "Count of assessed school students permitted to attend on a provisional basis while completing required immunizations.",
  statement = "In {location}, {value} school students were provisionally enrolled.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$pct_incomplete <- list(
  short_name = "Incomplete immunization rate",
  long_name = "Percent of students with incomplete immunizations",
  category = "immunization",
  short_description = "Percentage of assessed school students not up to date and not exempt.",
  long_description = "Share of assessed school students who have not completed the required immunizations and do not hold an exemption.",
  statement = "In {location}, {value}% of school students had incomplete immunizations.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$N_incomplete <- list(
  short_name = "Incomplete students (count)",
  long_name = "Number of students with incomplete immunizations",
  category = "immunization",
  short_description = "Number of assessed school students not up to date and not exempt.",
  long_description = "Count of assessed school students who have not completed the required immunizations and do not hold an exemption.",
  statement = "In {location}, {value} school students had incomplete immunizations.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$pct_missing <- list(
  short_name = "Missing documentation rate",
  long_name = "Percent of students with no immunization record on file",
  category = "immunization",
  short_description = "Percentage of assessed school students with no immunization documentation on file.",
  long_description = "Share of assessed school students for whom no immunization record was on file at the time of the survey.",
  statement = "In {location}, {value}% of school students had no immunization record on file.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_90_day <- list(
  short_name = "90-day provisional rate",
  long_name = "Percent of students in the 90-day provisional period",
  category = "immunization",
  short_description = "Percentage of students within the 90-day grace period to submit immunization documentation.",
  long_description = "Share of students within the 90-day provisional period allowed to submit immunization documentation after enrolling.",
  statement = "In {location}, {value}% of school students were in the 90-day provisional period.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_pme <- list(
  short_name = "Permanent medical exemption rate",
  long_name = "Percent of students with a permanent medical exemption",
  category = "immunization",
  short_description = "Percentage of students with a permanent medical exemption from immunization requirements.",
  long_description = "California kindergarten category: share of students granted a permanent medical exemption (PME) filed through CAIR-ME.",
  statement = "In {location}, {value}% of school students had a permanent medical exemption.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_other_lacking <- list(
  short_name = "Other / not up to date rate",
  long_name = "Percent of students lacking one or more required immunizations for other reasons",
  category = "immunization",
  short_description = "Percentage of students lacking one or more required immunizations for reasons other than exemption, conditional, or overdue status.",
  long_description = "School-entry assessment residual category: share of students not up to date and not otherwise classified as exempt, conditional, or overdue.",
  statement = "In {location}, {value}% of school students lacked one or more required immunizations for other reasons.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_overdue <- list(
  short_name = "Overdue rate",
  long_name = "Percent of students overdue for one or more required immunizations",
  category = "immunization",
  short_description = "Percentage of students past due for one or more required immunizations and not exempt or conditional.",
  long_description = "School-entry assessment category: share of students who are overdue for one or more required immunizations and are not otherwise exempt or conditionally enrolled.",
  statement = "In {location}, {value}% of school students were overdue for one or more required immunizations.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")

# state-specific exemption categories
MEASURES$pct_religious_exempt <- list(
  short_name = "Religious exemption rate",
  long_name = "Percent of students with a religious vaccination exemption",
  category = "immunization",
  short_description = "Percentage of assessed school students with a religious exemption from one or more required vaccines.",
  long_description = "Share of assessed school students holding a religious exemption. Reported by states that separate religious exemptions from other non-medical (philosophical or personal-belief) exemptions.",
  statement = "In {location}, {value}% of school students had a religious vaccination exemption.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$N_religious_exempt <- list(
  short_name = "Religious exemptions (count)",
  long_name = "Number of students with a religious vaccination exemption",
  category = "immunization",
  short_description = "Number of assessed school students with a religious exemption from one or more required vaccines.",
  long_description = "Count of assessed school students holding a religious exemption. Reported by states that separate religious exemptions from other non-medical exemptions.",
  statement = "In {location}, {value} school students had a religious vaccination exemption.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$pct_religious_membership_exempt <- list(
  short_name = "Religious membership exemption rate",
  long_name = "Percent of students with a religious membership exemption",
  category = "immunization",
  short_description = "Percentage of students exempt as members of a religious body opposed to immunization.",
  long_description = "Washington-specific category: share of students exempt as members of a religious body or church whose teachings are contrary to immunization, distinct from a personal religious exemption.",
  statement = "In {location}, {value}% of school students had a religious membership exemption.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$N_religious_membership_exempt <- list(
  short_name = "Religious membership exemptions (count)",
  long_name = "Number of students with a religious membership exemption",
  category = "immunization",
  short_description = "Number of students exempt as members of a religious body opposed to immunization.",
  long_description = "Washington-specific category: count of students exempt as members of a religious body or church whose teachings are contrary to immunization, distinct from a personal religious exemption.",
  statement = "In {location}, {value} school students had a religious membership exemption.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$pct_partial_medical_exempt_utd <- list(
  short_name = "Partial medical exemption, up to date (rate)",
  long_name = "Percent of students with a partial medical exemption who are otherwise up to date",
  category = "immunization",
  short_description = "Percentage of students with a medical exemption from some required vaccines who are up to date on the rest.",
  long_description = "Alabama category: share of students with a medical exemption from some but not all required vaccines who are up to date on the remaining required vaccines.",
  statement = "In {location}, {value}% of school students had a partial medical exemption and were up to date on the rest.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$N_partial_medical_exempt_utd <- list(
  short_name = "Partial medical exemption, up to date (count)",
  long_name = "Number of students with a partial medical exemption who are otherwise up to date",
  category = "immunization",
  short_description = "Number of students with a medical exemption from some required vaccines who are up to date on the rest.",
  long_description = "Alabama category: count of students with a medical exemption from some but not all required vaccines who are up to date on the remaining required vaccines.",
  statement = "In {location}, {value} school students had a partial medical exemption and were up to date on the rest.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$pct_partial_religious_exempt_utd <- list(
  short_name = "Partial religious exemption, up to date (rate)",
  long_name = "Percent of students with a partial religious exemption who are otherwise up to date",
  category = "immunization",
  short_description = "Percentage of students with a religious exemption from some required vaccines who are up to date on the rest.",
  long_description = "Alabama category: share of students with a religious exemption from some but not all required vaccines who are up to date on the remaining required vaccines.",
  statement = "In {location}, {value}% of school students had a partial religious exemption and were up to date on the rest.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$N_partial_religious_exempt_utd <- list(
  short_name = "Partial religious exemption, up to date (count)",
  long_name = "Number of students with a partial religious exemption who are otherwise up to date",
  category = "immunization",
  short_description = "Number of students with a religious exemption from some required vaccines who are up to date on the rest.",
  long_description = "Alabama category: count of students with a religious exemption from some but not all required vaccines who are up to date on the remaining required vaccines.",
  statement = "In {location}, {value} school students had a partial religious exemption and were up to date on the rest.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$pct_varicella_disease_history <- list(
  short_name = "Varicella disease-history rate",
  long_name = "Percent of students with a history of varicella disease",
  category = "immunization",
  short_description = "Percentage of students with a documented history of chickenpox accepted as evidence of immunity.",
  long_description = "Share of students with a documented history of varicella (chickenpox) disease, accepted in lieu of vaccination as evidence of immunity.",
  statement = "In {location}, {value}% of school students had a documented history of varicella disease.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_mmr_exempt <- list(
  short_name = "MMR exemption rate",
  long_name = "Percent of kindergarten students exempt from MMR vaccination",
  category = "immunization",
  short_description = "Percentage of kindergarten students with an exemption from the MMR vaccination requirement.",
  long_description = "Ohio publishes kindergarten MMR data as an exemption rate: the share of kindergarten students with a medical or non-medical exemption from the MMR requirement.",
  statement = "In {location}, {value}% of kindergarten students were exempt from MMR vaccination.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_completely_immunized <- list(
  short_name = "Completely immunized rate",
  long_name = "Percent of students completely immunized",
  category = "immunization",
  short_description = "Percentage of students at the school reported as completely immunized for all required vaccines.",
  long_description = "Share of enrolled students reported as completely immunized (all required vaccines) in the New York State School Immunization Survey. Reported at the individual-school level.",
  statement = "In {location}, {value}% of students were completely immunized.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_medical_exemptions <- MEASURES$pct_medical_exempt  # NY uses the plural column name

# Maine exemption categories (pct_exempt_<type>)
MEASURES$pct_exempt_total <- list(
  short_name = "Total exemption rate",
  long_name = "Percent of students with any vaccination exemption",
  category = "immunization",
  short_description = "Percentage of assessed school students with any exemption (medical or non-medical) from vaccination requirements.",
  long_description = "Share of assessed school students with at least one vaccination exemption of any type, as reported in the Maine School Vaccination Rates workbooks.",
  statement = "In {location}, {value}% of school students had a vaccination exemption of any kind.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_exempt_medical <- list(
  short_name = "Medical exemption rate",
  long_name = "Percent of students with a medical vaccination exemption",
  category = "immunization",
  short_description = "Percentage of assessed school students with a medical exemption from one or more required vaccines.",
  long_description = "Share of assessed school students with a physician-certified medical exemption from one or more required vaccines.",
  statement = "In {location}, {value}% of school students had a medical vaccination exemption.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_exempt_religious <- list(
  short_name = "Religious exemption rate",
  long_name = "Percent of students with a religious vaccination exemption",
  category = "immunization",
  short_description = "Percentage of assessed school students with a religious exemption from one or more required vaccines.",
  long_description = "Share of assessed school students holding a religious exemption from one or more required vaccines.",
  statement = "In {location}, {value}% of school students had a religious vaccination exemption.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
MEASURES$pct_exempt_philosophical <- list(
  short_name = "Philosophical exemption rate",
  long_name = "Percent of students with a philosophical vaccination exemption",
  category = "immunization",
  short_description = "Percentage of assessed school students with a philosophical (personal-belief) exemption from one or more required vaccines.",
  long_description = "Share of assessed school students holding a philosophical or personal-belief exemption from one or more required vaccines.",
  statement = "In {location}, {value}% of school students had a philosophical vaccination exemption.",
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")

# antigen-specific exemption rates -- Colorado (pct_<antigen>_<type>_exempt)
antigen_exempt <- function(label, type, statement_label = label) {
  tword <- if (type == "medical") "medical" else "non-medical"
  list(
    short_name = paste0(label, " ", tword, " exemption rate"),
    long_name = paste0("Percent of students with a ", tword, " exemption from ", label, " vaccination"),
    category = "immunization",
    short_description = paste0("Percentage of assessed school students with a ", tword,
      " exemption from the ", label, " vaccination requirement."),
    long_description = paste0("Share of assessed school students holding a ", tword,
      " exemption from the ", label, " vaccine specifically. Reported by states that break exemptions out by antigen."),
    statement = paste0("In {location}, {value}% of school students had a ", tword,
      " exemption from ", statement_label, " vaccination."),
    measure_type = "Percent", unit = "Percent", time_resolution = "Year")
}
co_antigens <- list(hepb = "hepatitis B", covid = "COVID-19", polio = "polio",
  hib = "Hib", varicella = "varicella", pcv = "pneumococcal (PCV)",
  mmr = "MMR", dtap = "DTaP", tdap = "Tdap")
for (a in names(co_antigens)) {
  for (ty in c("medical", "nonmedical")) {
    MEASURES[[paste0("pct_", a, "_", ty, "_exempt")]] <- antigen_exempt(co_antigens[[a]], ty)
  }
}
# antigen-specific exemption rates -- Minnesota (pct_<antigen>_<type>, no suffix)
mn_antigens <- list(dtap = "DTaP", polio = "polio", mmr = "MMR",
  hep_b = "hepatitis B", varicella = "varicella")
for (a in names(mn_antigens)) {
  for (ty in c("medical", "nonmedical")) {
    MEASURES[[paste0("pct_", a, "_", ty)]] <- antigen_exempt(mn_antigens[[a]], ty)
  }
}

# New York per-disease "immunized" columns
ny_immunized <- function(label) list(
  short_name = paste0(label, " immunization"),
  long_name = paste0("Percent of students immunized against ", label),
  category = "immunization",
  short_description = paste0("Percentage of students at the school reported as immunized against ", label, "."),
  long_description = paste0("Share of enrolled students reported as adequately immunized against ", label,
    " in the New York State School Immunization Survey. Reported at the individual-school level."),
  statement = paste0("In {location}, {value}% of students were immunized against ", label, "."),
  measure_type = "Percent", unit = "Percent", time_resolution = "Year")
ny_diseases <- list(polio = "polio", measles = "measles", mumps = "mumps",
  rubella = "rubella", diphtheria = "diphtheria", hepatitis_b = "hepatitis B",
  varicella = "varicella", tdap = "Tdap", meningococcal = "meningococcal disease")
for (a in names(ny_diseases)) {
  MEASURES[[paste0("pct_immunized_", a)]] <- ny_immunized(ny_diseases[[a]])
}

# California 7th-grade per-vaccine entrant-status columns
ca_vax <- list(tdap = "Tdap", varicella = "varicella")
for (a in names(ca_vax)) {
  lab <- ca_vax[[a]]
  MEASURES[[paste0("N_students_", a)]] <- m_denom(
    paste0(lab, " students assessed"),
    paste0("Number of 7th-grade students assessed for ", lab, " immunization"),
    paste0("Number of 7th-grade students assessed for ", lab, " immunization status."),
    paste0("Count of 7th-grade students assessed for ", lab, " immunization; the denominator for the accompanying ", lab, " percentages."),
    paste0("In {location}, {value} 7th-grade students were assessed for ", lab, " immunization."))
  MEASURES[[paste0("pct_entrants_vax_", a)]] <- list(
    short_name = paste0(lab, " coverage (7th grade)"),
    long_name = paste0("Percent of 7th-grade students up to date for ", lab, " vaccination"),
    category = "immunization",
    short_description = paste0("Percentage of 7th-grade students up to date for ", lab, " vaccination."),
    long_description = paste0("Share of 7th-grade students reported as up to date for ", lab,
      " vaccination in the California school immunization data, enrollment-weighted to the county."),
    statement = paste0("In {location}, {value}% of 7th-grade students were up to date for ", lab, " vaccination."),
    measure_type = "Percent", unit = "Percent", time_resolution = "Year")
  MEASURES[[paste0("pct_conditional_", a)]] <- list(
    short_name = paste0(lab, " conditional rate (7th grade)"),
    long_name = paste0("Percent of 7th-grade students conditionally enrolled for ", lab),
    category = "immunization",
    short_description = paste0("Percentage of 7th-grade students conditionally enrolled while completing ", lab, " requirements."),
    long_description = paste0("Share of 7th-grade students conditionally enrolled with respect to the ", lab, " requirement (in process, not overdue)."),
    statement = paste0("In {location}, {value}% of 7th-grade students were conditionally enrolled for ", lab, "."),
    measure_type = "Percent", unit = "Percent", time_resolution = "Year")
  MEASURES[[paste0("pct_pme_", a)]] <- list(
    short_name = paste0(lab, " permanent medical exemption rate (7th grade)"),
    long_name = paste0("Percent of 7th-grade students with a permanent medical exemption for ", lab),
    category = "immunization",
    short_description = paste0("Percentage of 7th-grade students with a permanent medical exemption from the ", lab, " requirement."),
    long_description = paste0("Share of 7th-grade students granted a permanent medical exemption (PME) from the ", lab, " requirement."),
    statement = paste0("In {location}, {value}% of 7th-grade students had a permanent medical exemption for ", lab, "."),
    measure_type = "Percent", unit = "Percent", time_resolution = "Year")
  MEASURES[[paste0("pct_other_lacking_", a)]] <- list(
    short_name = paste0(lab, " other / not up to date rate (7th grade)"),
    long_name = paste0("Percent of 7th-grade students lacking ", lab, " for other reasons"),
    category = "immunization",
    short_description = paste0("Percentage of 7th-grade students not up to date for ", lab, " for reasons other than exemption, conditional, or overdue status."),
    long_description = paste0("Residual category: share of 7th-grade students not up to date for ", lab, " and not otherwise classified."),
    statement = paste0("In {location}, {value}% of 7th-grade students lacked ", lab, " for other reasons."),
    measure_type = "Percent", unit = "Percent", time_resolution = "Year")
  MEASURES[[paste0("pct_overdue_", a)]] <- list(
    short_name = paste0(lab, " overdue rate (7th grade)"),
    long_name = paste0("Percent of 7th-grade students overdue for ", lab),
    category = "immunization",
    short_description = paste0("Percentage of 7th-grade students overdue for the ", lab, " vaccination and not exempt or conditional."),
    long_description = paste0("Share of 7th-grade students past due for ", lab, " vaccination and not otherwise exempt or conditionally enrolled."),
    statement = paste0("In {location}, {value}% of 7th-grade students were overdue for ", lab, "."),
    measure_type = "Percent", unit = "Percent", time_resolution = "Year")
}

# ---- per-state source registry ----------------------------------------------
# One entry per ingested state, compiled from data/DATA_SOURCES.md. add_src()
# assigns a source_id and records both the state->source_id map and the
# _sources entry.

STATE_SOURCE <- list()
SOURCES <- list()
add_src <- function(abbr, name, url, organization, organization_url, description,
                    restrictions = "Public data published by the source agency; no access restrictions.") {
  sid <- paste0(tolower(abbr), "_school_immunization")
  STATE_SOURCE[[abbr]] <<- sid
  SOURCES[[sid]] <<- list(name = name, url = url, organization = organization,
    organization_url = organization_url, description = description,
    restrictions = restrictions)
}

add_src("OR", "Oregon School Immunization Coverage, K-12",
  "https://www.oregon.gov/oha/PH/PREVENTIONWELLNESS/VACCINESIMMUNIZATION/GETTINGIMMUNIZED/Documents/SchK-12.xlsx",
  "Oregon Health Authority, Public Health Division", "https://www.oregon.gov/oha/PH/",
  "Statewide K-12 school immunization workbook published each fall by the Oregon Health Authority (OHA). School-level rows carry the county (Agency), adjusted enrollment, and per-antigen coverage and exemption percentages; this ingest enrollment-weights the school-level rates up to the county level. OHA overwrites the workbook at a fixed URL each year, so the series is self-updating.",
  "Public data published by the Oregon Health Authority; no access restrictions. County values are enrollment-weighted aggregates of school-level rates and may differ slightly from any official county figures OHA reports separately.")

add_src("CA", "California School Immunization Assessment (Kindergarten and 7th grade)",
  "https://data.chhs.ca.gov/dataset/school-immunizations-in-kindergarten-by-academic-year",
  "California Department of Public Health", "https://www.cdph.ca.gov/",
  "County-level kindergarten and 7th-grade immunization status assembled from CHHS Open Data school-level files (enrollment-weighted to county) plus the CDPH official county report for the most recent year.",
  "Public data (CHHS Open Data and CDPH). County aggregates from the CHHS school-level files are enrollment-weighted and fall within about one percentage point of official CDPH figures because school percentages are integer-rounded.")

add_src("CO", "Colorado School and Child Care Immunization and Exemption Data",
  "https://data.colorado.gov/dataset/CDPHE-Colorado-School-and-Child-Care-Immunization-/3b5w-8ggf",
  "Colorado Department of Public Health and Environment", "https://cdphe.colorado.gov/",
  "County-level antigen-specific medical and non-medical exemption rates from CDPHE, pulled from the Colorado Information Marketplace (Socrata 3b5w-8ggf) / CDPHE ArcGIS Open Data.")

add_src("CT", "Connecticut School Immunization and Exemption Rates",
  "https://data.ct.gov/Health-and-Human-Services/2025-2026-Vaccine-Exemption-Rates-by-School-All-Gr/a2a4-pw6c",
  "Connecticut Department of Public Health", "https://portal.ct.gov/dph",
  "County / county-equivalent coverage and exemption counts from the CT Open Data school immunization datasets (Socrata), with a crosswalk that handles the 2022+ county-to-planning-region transition.")

add_src("NY", "New York State School Immunization Survey",
  "https://health.data.ny.gov/Health/School-Immunization-Survey-Beginning-2019-20-Schoo/btkd-y8bp",
  "New York State Department of Health", "https://www.health.ny.gov/",
  "School-level immunization and medical-exemption percentages from the NYS School Immunization Survey, pulled from the Health Data NY open-data API (CSV export).")

add_src("NM", "New Mexico School Immunization Coverage",
  "https://www.arcgis.com/apps/dashboards/c40e909922a243968807dc7b10870405",
  "New Mexico Department of Health", "https://www.nmhealth.org/",
  "County-level kindergarten and 7th-grade coverage and exemption data from the NMDOH ArcGIS dashboard feature service.")

add_src("RI", "Rhode Island School Immunization Coverage",
  "https://ricair-data-rihealth.hub.arcgis.com/",
  "Rhode Island Department of Health", "https://health.ri.gov/",
  "County-level coverage and exemption data from the RICAIR ArcGIS Hub (CSV/GeoJSON download and REST feature service).")

add_src("PA", "Pennsylvania School Immunizations - County Survey",
  "https://www.pa.gov/agencies/health/programs/immunizations/rates",
  "Pennsylvania Department of Health", "https://www.pa.gov/agencies/health.html",
  "County-level immunization and exemption data from the annual PA DOH 'by County' school immunization survey workbooks (kindergarten, 7th, and 12th grade).")

add_src("MN", "Minnesota School Immunization Data",
  "https://www.health.state.mn.us/people/immunize/stats/school/index.html",
  "Minnesota Department of Health", "https://www.health.state.mn.us/",
  "County-level kindergarten coverage plus antigen-specific medical and non-medical exemption data from the MDH per-year county workbooks.")

add_src("MD", "Maryland Kindergarten Immunization Data",
  "https://health.maryland.gov/phpa/OIDEOR/IMMUN/Pages/Kindergarten_Immunization_Rates_by_School.aspx",
  "Maryland Department of Health", "https://health.maryland.gov/",
  "County-level kindergarten coverage aggregated (enrollment-weighted) from the Maryland by-school immunization workbooks.",
  "Public data. County values are enrollment-weighted aggregates of the by-school workbooks, used because Maryland publishes county tables only as PDF.")

add_src("MA", "Massachusetts School Immunization Survey",
  "https://www.mass.gov/info-details/school-immunizations",
  "Massachusetts Department of Public Health", "https://www.mass.gov/orgs/department-of-public-health",
  "County-level kindergarten and 7th-grade coverage and exemption data from the annual MDPH by-county school immunization workbooks.")

add_src("ME", "Maine School Vaccination Rates",
  "https://www.maine.gov/dhhs/mecdc/data-reports/immunization",
  "Maine Center for Disease Control and Prevention", "https://www.maine.gov/dhhs/mecdc",
  "School-level kindergarten, 7th- and 12th-grade coverage and exemption data from the Maine CDC 'School Vaccination Rates' workbooks.")

add_src("HI", "Hawaii School Immunization and Exemption Data",
  "https://health.hawaii.gov/docd/resources/reports/immunization-examination-requirements/",
  "Hawaii State Department of Health", "https://health.hawaii.gov/",
  "County-level school immunization and exemption data from the Hawaii DOH immunization/examination requirements reports.")

add_src("IA", "Iowa School and Child Care Immunization Audit",
  "https://hhs.iowa.gov/about/data-reports/health-disease/immunization/school-child-care-audits",
  "Iowa Department of Health and Human Services", "https://hhs.iowa.gov/",
  "County-level K-12 immunization and exemption data from the annual Iowa HHS school and child-care audit reports.")

add_src("IL", "Illinois School Immunization Data",
  "https://www.isbe.net/Pages/Health-Requirements-Student-Data.aspx",
  "Illinois State Board of Education", "https://www.isbe.net/",
  "County-level immunization and exemption data from the ISBE public-use immunization data files.")

add_src("IN", "Indiana School Immunization Coverage",
  "https://hub.mph.in.gov/dataset/immunization-division-s-school-supplemental-dashboard",
  "Indiana Department of Health", "https://www.in.gov/health/",
  "County-level, multi-grade antigen coverage from the Indiana Department of Health open-data hub (CKAN); per-year workbooks are enumerated via the package_show API so the series self-updates.",
  "Public data. The hub files carry no medical/religious exemption split, so exemption columns are unavailable (NA).")

add_src("KS", "Kansas Kindergarten Immunization Data",
  "https://www.kdhe.ks.gov/2016/Kindergarten-Immunization-Data",
  "Kansas Department of Health and Environment", "https://www.kdhe.ks.gov/",
  "County-level kindergarten immunization and exemption data from the annual KDHE reports.")

add_src("KY", "Kentucky School Immunization Data",
  "https://www.chfs.ky.gov/agencies/dph/dehp/Pages/immunization.aspx",
  "Kentucky Cabinet for Health and Family Services", "https://www.chfs.ky.gov/",
  "County-level school immunization and exemption data from the Kentucky Department for Public Health.")

add_src("LA", "Louisiana School Immunization Coverage",
  "https://ldh.la.gov/immunization-program/vaccination-data-resources",
  "Louisiana Department of Health", "https://ldh.la.gov/",
  "Parish-level coverage and exemption data from a multi-year Louisiana Office of Public Health school immunization workbook.",
  "Public data. Ingested from a committed multi-year parish workbook because the LDH online dashboard is behind authentication.")

add_src("MI", "Michigan School Immunization Data",
  "https://www.michigan.gov/en/mdhhs/adult-child-serv/childrenfamilies/Immunizations/Data-Statistics/school-immunization-data",
  "Michigan Department of Health and Human Services", "https://www.michigan.gov/mdhhs",
  "School- and county-level immunization completeness and exemption data from the MDHHS building-level immunization files.")

add_src("MS", "Mississippi School Immunization Data",
  "https://msdh.ms.gov/page/14,0,71,688.html",
  "Mississippi State Department of Health", "https://msdh.ms.gov/",
  "County-level school immunization and exemption data from the annual MSDH reports (religious exemptions permitted after July 2023).")

add_src("MT", "Montana School Immunization Data",
  "https://dphhs.mt.gov/publichealth/immunization/childcareandschoolresources",
  "Montana Department of Public Health and Human Services", "https://dphhs.mt.gov/",
  "County-level immunization completeness and exemption data from the Montana DPHHS school immunization reports.",
  "Public data. Montana ended routine school immunization data collection after the 2018-19 school year.")

add_src("NH", "New Hampshire School Immunization Data",
  "https://www.dhhs.nh.gov/programs-services/disease-prevention/nh-immunization-program/immunization-guidance-schools",
  "New Hampshire Department of Health and Human Services", "https://www.dhhs.nh.gov/",
  "County-level immunization status (up to date, conditional, exempt) from the annual NH DHHS school immunization reports.")

add_src("TX", "Texas School Immunization Coverage",
  "https://www.dshs.texas.gov/immunizations/data/school",
  "Texas Department of State Health Services", "https://www.dshs.texas.gov/",
  "County-level school immunization and exemption data from the annual DSHS Annual Report of Immunization Status.")

add_src("VA", "Virginia School Immunization Compliance",
  "https://www.vdh.virginia.gov/immunization/datamanagement/sisreports/",
  "Virginia Department of Health", "https://www.vdh.virginia.gov/",
  "County-level school immunization compliance and exemption data from the VDH School Immunization Survey summaries.")

add_src("SC", "South Carolina School Vaccination Coverage",
  "https://dph.sc.gov/health-wellness/child-teen-health/vaccine-requirements-info/school-vaccination-coverage-data",
  "South Carolina Department of Public Health", "https://dph.sc.gov/",
  "County-level school vaccination coverage and exemption data from SC DPH reports.")

add_src("OK", "Oklahoma School Immunization Data",
  "https://oklahoma.gov/health/services/personal-health/immunizations.html",
  "Oklahoma State Department of Health", "https://oklahoma.gov/health.html",
  "County-level school immunization and exemption data from the annual OSDH reports and county map.")

add_src("AZ", "Arizona School Immunization Coverage",
  "https://apps.azdhs.gov/IDRReportStats",
  "Arizona Department of Health Services", "https://www.azdhs.gov/",
  "County-level school immunization and exemption data from the ADHS immunization reporting query tool.")

add_src("TN", "Tennessee Kindergarten MMR Coverage",
  "https://www.tn.gov/health/cedep/immunization-program.html",
  "Tennessee Department of Health", "https://www.tn.gov/health.html",
  "County-level kindergarten MMR coverage supplied directly to PopHIVE by the Tennessee Department of Health (percent of kindergartners fully immunized for MMR).",
  "Public data provided by the Tennessee Department of Health. Single-cohort snapshot; kindergarten MMR only. Do not substitute the Washington Post Tableau/PDF source.")

add_src("FL", "Florida School Immunization Data",
  "https://www.flhealthcharts.gov/charts/CommunicableDiseases/default.aspx",
  "Florida Department of Health", "https://www.floridahealth.gov/",
  "County-level school immunization data from Florida Health Charts.")

add_src("ID", "Idaho School Immunization Report",
  "https://www.gethealthy.dhw.idaho.gov/idaho-school-immunization-report",
  "Idaho Department of Health and Welfare", "https://healthandwelfare.idaho.gov/",
  "County-level school immunization and exemption data from the Idaho DHW school immunization report.")

add_src("MO", "Missouri School Immunization Data",
  "https://health.mo.gov/living/families/schoolhealth/dashboard.php",
  "Missouri Department of Health and Senior Services", "https://health.mo.gov/",
  "County-level school immunization and exemption data from the Missouri DHSS school immunization dashboard.")

add_src("NC", "North Carolina Kindergarten Immunization Data",
  "https://www.dph.ncdhhs.gov/programs/epidemiology/immunization/data/kindergarten-dashboard",
  "North Carolina Department of Health and Human Services", "https://www.dph.ncdhhs.gov/",
  "County-level kindergarten immunization coverage and exemption data from the NC DPH kindergarten dashboard.")

add_src("ND", "North Dakota School Immunization Coverage",
  "https://www.hhs.nd.gov/immunizations/coverage-rates",
  "North Dakota Health and Human Services", "https://www.hhs.nd.gov/",
  "County-level school immunization coverage, incompletion, and exemption data from the ND HHS coverage-rate reports.")

add_src("NJ", "New Jersey School Immunization Status",
  "https://www.nj.gov/health/cd/statistics/imm-status-reports/",
  "New Jersey Department of Health", "https://www.nj.gov/health/",
  "County-level school immunization and exemption data from the NJDOH immunization status reports.")

add_src("OH", "Ohio Kindergarten Immunization Assessment",
  "https://data.ohio.gov/wps/portal/gov/data/view/annual-ohio-kindergarten-immunization-level-assessment",
  "Ohio Department of Health", "https://odh.ohio.gov/",
  "County-level kindergarten MMR exemption rates from the annual Ohio Kindergarten Immunization Level Assessment.")

add_src("SD", "South Dakota School Immunization Data",
  "https://doh.sd.gov/health-data-reports/data-dashboards/school-immunization-dashboard",
  "South Dakota Department of Health", "https://doh.sd.gov/",
  "School-level immunization counts by antigen and exemption from the South Dakota DOH school immunization data.")

add_src("UT", "Utah School Immunization Coverage",
  "https://immunize.utah.gov/information-for-the-public/utah-statistics/",
  "Utah Department of Health and Human Services", "https://dhhs.utah.gov/",
  "County- and health-district-level school immunization and exemption data from the Utah DHHS coverage reports.")

add_src("VT", "Vermont School Vaccination Data",
  "https://www.healthvermont.gov/stats/surveillance-reporting-topic/school-vaccination-data",
  "Vermont Department of Health", "https://www.healthvermont.gov/",
  "School- and county-level immunization completeness, provisional, and exemption data from the Vermont DOH school vaccination reports.",
  "Public data. Vermont suppresses small counts to protect privacy.")

add_src("WA", "Washington School Immunization Data",
  "https://doh.wa.gov/data-and-statistical-reports/washington-tracking-network-wtn/school-immunization",
  "Washington State Department of Health", "https://doh.wa.gov/",
  "County-level school immunization coverage and exemption data, including religious membership exemptions, from the WA DOH school immunization reports.")

add_src("WI", "Wisconsin School Immunization Data",
  "https://www.dhs.wisconsin.gov/library/collection/p-01892",
  "Wisconsin Department of Health Services", "https://www.dhs.wisconsin.gov/",
  "County-level school immunization and exemption data from the Wisconsin DHS student immunization reports.")

add_src("AL", "Alabama School-Entry Immunization Data",
  "https://www.alabamapublichealth.gov/immunization/school-entry-survey.html",
  "Alabama Department of Public Health", "https://www.alabamapublichealth.gov/",
  "County-level school-entry immunization and exemption data, including partial-exemption categories, from the Alabama DPH school-entry survey.")

add_src("AR", "Arkansas School Immunization Data",
  "https://healthy.arkansas.gov/programs-services/community-family-child-health/immunizations/",
  "Arkansas Department of Health", "https://healthy.arkansas.gov/",
  "County-level school immunization and exemption data from the Arkansas Department of Health.")

add_src("NV", "Nevada School Immunization Coverage",
  "https://www.dpbh.nv.gov/programs/immunizations/school-and-child-care-immunizations/",
  "Nevada Division of Public and Behavioral Health", "https://dpbh.nv.gov/",
  "County-level school immunization coverage from the Nevada DPBH; the MMR series is consistent with CDC SchoolVaxView.")

add_src("WV", "West Virginia School Immunization Exemptions",
  "https://oeps.wv.gov/immunizations/Pages/school_coverage_rates.aspx",
  "West Virginia Department of Health", "https://dhhr.wv.gov/",
  "County-level school vaccination exemption counts by vaccine from the West Virginia Office of Epidemiology and Prevention Services.",
  "2025 exemption counts obtained via a public-records (FOIA) request; not published as a downloadable dataset.")

# ---- index/dimension columns (never emitted as measures) ---------------------
INDEX_COLS <- c(
  "time", "state", "geography", "geography_name", "county", "grade",
  "school_name", "school_id", "district", "school_type", "source_grade",
  "health_district", "unit", "public_independent", "vaccine"
)

# ---- build one state ---------------------------------------------------------
read_header <- function(path) {
  con <- gzfile(path, "r")
  on.exit(close(con))
  line <- readLines(con, n = 1L, warn = FALSE)
  sep <- if (grepl("\t", line, fixed = TRUE)) "\t" else ","
  strsplit(line, sep, fixed = TRUE)[[1]]
}

build_state <- function(abbr, data_path, out_path) {
  cols <- read_header(data_path)
  source_id <- STATE_SOURCE[[abbr]]
  if (is.null(source_id)) {
    message("  [skip] ", abbr, ": no source registered")
    return(invisible(FALSE))
  }
  measure_cols <- setdiff(cols, INDEX_COLS)
  unknown <- setdiff(measure_cols, names(MEASURES))
  if (length(unknown)) {
    message("  [warn] ", abbr, ": no dictionary entry for -> ",
            paste(unknown, collapse = ", "))
  }
  known <- intersect(measure_cols, names(MEASURES))
  if (!length(known)) {
    message("  [skip] ", abbr, ": no known measure columns")
    return(invisible(FALSE))
  }

  out <- list()
  for (id in known) {
    entry <- MEASURES[[id]]
    out[[id]] <- c(list(id = id), entry, list(sources = list(list(id = source_id))))
  }
  out[["_sources"]] <- list()
  out[["_sources"]][[source_id]] <- SOURCES[[source_id]]

  writeLines(emit_json(out, 0L), out_path)
  message("  [ok]   ", abbr, ": ", length(known), " measures -> ", out_path)
  invisible(TRUE)
}

# ---- main --------------------------------------------------------------------
main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  data_root <- "data"
  states <- list.dirs(data_root, recursive = FALSE, full.names = FALSE)
  if (length(args)) states <- intersect(states, args)
  for (abbr in states) {
    data_path <- file.path(data_root, abbr, "standard", "data.csv.gz")
    if (!file.exists(data_path)) next
    out_path <- file.path(data_root, abbr, "measure_info.json")
    build_state(abbr, data_path, out_path)
  }
}

main()
