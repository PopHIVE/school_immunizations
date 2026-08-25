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
# Shares are RATES on a 0-1 scale, in rate_* columns, matching
# resources/rate_scale.R -- so measure_type and unit are "Rate", and the
# statement templates carry no percent sign.
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
    long_name = paste0("Proportion of students up to date for ", long_antigen, " vaccination"),
    category = "immunization",
    short_description = paste0(
      "Proportion of assessed school students who are up to date for ",
      long_antigen, " vaccination."),
    long_description = paste0(
      "Share of assessed school students reported as up to date for ",
      long_antigen, " vaccination in the state's annual school immunization ",
      "survey. The antigen-specific series definition, the grade(s) assessed, ",
      "and whether the figure reflects all enrolled students or only those ",
      "surveyed vary by state; see the source description."),
    statement = paste0("In {location}, {value} of school students were up to date for ",
      statement_antigen, " vaccination."),
    measure_type = "Rate",
    unit = "Rate",
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
  rate_dtap = m_coverage("DTaP/Tdap", "diphtheria-tetanus-pertussis (DTaP/Tdap)", "DTaP/Tdap"),
  rate_polio = m_coverage("Polio", "polio (IPV/OPV)", "polio"),
  rate_mmr = m_coverage("MMR", "measles-mumps-rubella (MMR)", "MMR"),
  rate_hep_b = m_coverage("Hepatitis B", "hepatitis B", "hepatitis B"),
  rate_varicella = m_coverage("Varicella", "varicella (chickenpox)", "varicella"),

  # antigen coverage (counts)
  N_dtap = m_count("DTaP/Tdap", "diphtheria-tetanus-pertussis (DTaP/Tdap)"),
  N_polio = m_count("Polio", "polio (IPV/OPV)"),
  N_mmr = m_count("MMR", "measles-mumps-rubella (MMR)"),
  N_hep_b = m_count("Hepatitis B", "hepatitis B"),
  N_varicella = m_count("Varicella", "varicella (chickenpox)"),

  # exemptions (percent)
  rate_personal_exempt = list(
    short_name = "Non-medical exemption rate",
    long_name = "Proportion of students with a non-medical (personal-belief) vaccination exemption",
    category = "immunization",
    short_description = "Proportion of assessed school students with a non-medical exemption (religious, philosophical, or personal-belief) from vaccination requirements.",
    long_description = "Share of assessed school students holding a non-medical exemption from one or more required vaccines. The exemption categories permitted differ by state law - some allow religious exemptions only, others also allow philosophical or personal-belief exemptions - and this harmonized field aggregates whichever non-medical categories the state reports.",
    statement = "In {location}, {value} of school students had a non-medical vaccination exemption.",
    measure_type = "Rate",
    unit = "Rate",
    time_resolution = "Year"
  ),
  rate_conscientious_exemption = list(
    short_name = "Conscientious exemption rate",
    long_name = "Proportion of students with a conscientious vaccination exemption",
    category = "immunization",
    short_description = "Proportion of assessed school students with a conscientious exemption from vaccination requirements, as defined under Texas law.",
    long_description = "Share of assessed school students holding a conscientious exemption (an affidavit declining vaccination for reasons of conscience, including religious belief) from one or more required vaccines. Texas tracks this affidavit-based exemption separately from medical exemptions, which are not captured in this source.",
    statement = "In {location}, {value} of school students had a conscientious vaccination exemption.",
    measure_type = "Rate",
    unit = "Rate",
    time_resolution = "Year"
  ),
  rate_medical_exempt = list(
    short_name = "Medical exemption rate",
    long_name = "Proportion of students with a medical vaccination exemption",
    category = "immunization",
    short_description = "Proportion of assessed school students with a medical exemption from one or more required vaccines.",
    long_description = "Share of assessed school students with a physician-certified medical exemption from one or more required vaccines, as reported in the state's annual school immunization survey.",
    statement = "In {location}, {value} of school students had a medical vaccination exemption.",
    measure_type = "Rate",
    unit = "Rate",
    time_resolution = "Year"
  ),
  rate_full_exempt = list(
    short_name = "Total exemption rate",
    long_name = "Proportion of students with any vaccination exemption",
    category = "immunization",
    short_description = "Proportion of assessed school students with any exemption (medical or non-medical) from vaccination requirements.",
    long_description = "Share of assessed school students with at least one vaccination exemption of any type, combining medical and non-medical exemptions. Where a state reports the categories separately this is their sum; overlap handling depends on what the source publishes.",
    statement = "In {location}, {value} of school students had a vaccination exemption of any kind.",
    measure_type = "Rate",
    unit = "Rate",
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
MEASURES$rate_tdap <- m_coverage("Tdap", "tetanus-diphtheria-pertussis (Tdap)", "Tdap")
MEASURES$rate_hep_a <- m_coverage("Hepatitis A", "hepatitis A", "hepatitis A")
MEASURES$rate_mcv4 <- m_coverage("Meningococcal (MCV4)", "meningococcal conjugate (MenACWY/MCV4)", "meningococcal (MenACWY)")
MEASURES$rate_menacwy <- m_coverage("Meningococcal (MenACWY)", "meningococcal conjugate (MenACWY)", "meningococcal (MenACWY)")
MEASURES$rate_hib_utd <- m_coverage("Hib", "Haemophilus influenzae type b (Hib)", "Hib")
MEASURES$rate_pcv_utd <- m_coverage("Pneumococcal (PCV)", "pneumococcal conjugate (PCV)", "pneumococcal (PCV)")
MEASURES$rate_hpv <- m_coverage("HPV", "human papillomavirus (HPV)", "HPV")
MEASURES$N_tdap <- m_count("Tdap", "tetanus-diphtheria-pertussis (Tdap)")

# Spellings used by states whose per-vaccine columns are named to match their
# N_<vaccine>_enrolled denominators (Colorado), where the antigen key has no
# "_utd" suffix. Aliases of the entries above rather than new prose, so the
# two spellings cannot drift apart.
MEASURES$rate_hib <- MEASURES$rate_hib_utd
MEASURES$rate_pcv <- MEASURES$rate_pcv_utd
MEASURES$rate_covid <- m_coverage("COVID-19", "COVID-19", "COVID-19")
MEASURES$N_menacwy <- m_count("MenACWY", "meningococcal conjugate (MenACWY)")
MEASURES$N_hep_a <- m_count("Hepatitis A", "hepatitis A")

# ---- per-vaccine exemption counts (WV) --------------------------------------
# West Virginia publishes an exemption count per vaccine and no enrolment, so
# these have no matching rate. A pupil exempt from several vaccines is counted
# in each of their columns, so they do not add up to a number of exempt pupils.
#
# Every vaccine also has a grade stratum "all, permanent", read from the second
# table on its sheet: permanent MEDICAL exemptions in force that year, all grades
# together. It is a standing count on a calendar-year clock, so it is a separate
# stratum and not additive with the kindergarten/7th/12th rows it partly
# duplicates. See data/WV/ingest.R.
m_vax_exempt <- function(short_name, long_name) {
  list(short_name = paste(short_name, "exemptions"),
       long_name = paste0("Number of students with a ", long_name, " exemption"),
       category = "immunization",
       short_description = paste0("Students exempted from the ", long_name,
                                  " requirement."),
       long_description = paste0(
         "Count of students with an exemption from the ", long_name,
         " requirement. Published without an enrollment denominator, so no rate ",
         "is derived. A student exempt from more than one vaccine is counted ",
         "once per vaccine, so these counts do not sum to a number of exempt ",
         "students. West Virginia additionally reports a grade stratum ",
         "\"all, permanent\": the number of permanent medical exemptions in ",
         "force in that year across all grades. Those rows are a standing count ",
         "rather than a per-year addition, and must not be added to the ",
         "kindergarten, 7th- and 12th-grade rows, which they partly duplicate."),
       statement = paste0("In {location}, {value} students had a ", long_name,
                          " exemption."),
       measure_type = "Count", unit = "Students", time_resolution = "Year")
}
MEASURES$N_mmr_exempt <- m_vax_exempt("MMR", "measles-mumps-rubella (MMR)")
MEASURES$N_dtap_exempt <- m_vax_exempt("DTaP", "diphtheria-tetanus-pertussis (DTaP)")
MEASURES$N_tdap_exempt <- m_vax_exempt("Tdap", "tetanus-diphtheria-pertussis (Tdap)")
MEASURES$N_hib_exempt <- m_vax_exempt("Hib", "Haemophilus influenzae type b (Hib)")
MEASURES$N_varicella_exempt <- m_vax_exempt("Varicella", "varicella")
MEASURES$N_menacwy_exempt <- m_vax_exempt("MenACWY", "meningococcal conjugate (MenACWY)")
MEASURES$N_ipv_exempt <- m_vax_exempt("Polio (IPV)", "inactivated poliovirus (IPV)")
MEASURES$N_pcv_exempt <- m_vax_exempt("Pneumococcal (PCV)", "pneumococcal conjugate (PCV)")
MEASURES$N_hep_exempt <- m_vax_exempt("Hepatitis", "hepatitis")

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

# ---- per-vaccine enrollment denominators (CO) -------------------------------
# CDPHE reports enrollment per vaccine rather than per record, because a vaccine
# is only required in the grades it applies to: in Adams County 2021/2022 the
# Tdap rows carry 47,061 against 85,200 for the K-12 vaccines. Each vaccine's own
# denominator is therefore kept under its own name.
m_vax_denom <- function(short_name, long_name) {
  m_denom(
    paste(short_name, "enrollment"),
    paste0("Number of students enrolled in the grades where ", long_name,
           " is required"),
    paste0("Enrollment used as the denominator for the ", long_name,
           " percentages."),
    paste0("Count of students enrolled in the grades subject to the ", long_name,
           " requirement. It is the denominator of that vaccine's percentages ",
           "in the same record, and differs between vaccines because they are ",
           "required at different grades."),
    paste0("In {location}, {value} students were enrolled in grades where ",
           long_name, " is required."))
}
MEASURES$N_mmr_enrolled <- m_vax_denom("MMR", "measles-mumps-rubella (MMR)")
MEASURES$N_dtap_enrolled <- m_vax_denom("DTaP", "diphtheria-tetanus-pertussis (DTaP)")
MEASURES$N_tdap_enrolled <- m_vax_denom("Tdap", "tetanus-diphtheria-pertussis (Tdap)")
MEASURES$N_hib_enrolled <- m_vax_denom("Hib", "Haemophilus influenzae type b (Hib)")
MEASURES$N_varicella_enrolled <- m_vax_denom("Varicella", "varicella")
MEASURES$N_polio_enrolled <- m_vax_denom("Polio", "poliovirus")
MEASURES$N_pcv_enrolled <- m_vax_denom("Pneumococcal (PCV)", "pneumococcal conjugate (PCV)")
MEASURES$N_hep_b_enrolled <- m_vax_denom("Hepatitis B", "hepatitis B")
MEASURES$N_covid_enrolled <- m_vax_denom("COVID-19", "COVID-19 vaccination")
MEASURES$N_students <- m_denom(
  "Students assessed", "Number of students assessed",
  "Number of students (entrants) assessed for immunization status.",
  "Count of students (school entrants) assessed for immunization status; the enrollment-based denominator for the accompanying percentages.",
  "In {location}, {value} students were assessed.")

# immunization / enrollment status categories
MEASURES$rate_complete <- list(
  short_name = "Up-to-date rate",
  long_name = "Proportion of students up to date for all required vaccines",
  category = "immunization",
  short_description = "Proportion of assessed school students up to date for all required vaccines.",
  long_description = "Share of assessed school students who have completed all age-appropriate required immunizations (fully immunized / up to date at the time of the survey).",
  statement = "In {location}, {value} of school students were up to date for all required vaccines.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$N_complete <- list(
  short_name = "Up-to-date students (count)",
  long_name = "Number of students up to date for all required vaccines",
  category = "immunization",
  short_description = "Number of assessed school students up to date for all required vaccines.",
  long_description = "Count of assessed school students who have completed all age-appropriate required immunizations.",
  statement = "In {location}, {value} school students were up to date for all required vaccines.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$rate_all_required <- list(
  short_name = "All-required coverage",
  long_name = "Proportion of students with all required immunizations",
  category = "immunization",
  short_description = "Proportion of students reported as having all required immunizations at school entry.",
  long_description = "School-entry assessment category (e.g. California kindergarten reporting): share of students up to date with all required immunizations at entry.",
  statement = "In {location}, {value} of students had all required immunizations.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_conditional <- list(
  short_name = "Conditional enrollment rate",
  long_name = "Proportion of students conditionally enrolled",
  category = "immunization",
  short_description = "Proportion of assessed school students admitted conditionally while completing required immunizations.",
  long_description = "Share of assessed school students who are conditionally (or provisionally) enrolled - permitted to attend while in the process of completing required immunizations and not yet overdue.",
  statement = "In {location}, {value} of school students were conditionally enrolled.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
# Named for the source category rather than "provisional enrollment": Vermont's
# workbook prints "Provisional Admittance", which is a distinct legal status
# (admitted while the required doses are outstanding) and not the same thing as
# rate_conditional above.
MEASURES$rate_provisional_admittance <- list(
  short_name = "Provisional admittance rate",
  long_name = "Proportion of students provisionally admitted",
  category = "immunization",
  short_description = "Proportion of assessed school students provisionally admitted while completing required immunizations.",
  long_description = "Share of assessed school students permitted to attend on a provisional basis while completing required immunizations.",
  statement = "In {location}, {value} of school students were provisionally admitted.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$N_provisional <- list(
  short_name = "Provisional students (count)",
  long_name = "Number of students provisionally enrolled",
  category = "immunization",
  short_description = "Number of assessed school students provisionally enrolled while completing required immunizations.",
  long_description = "Count of assessed school students permitted to attend on a provisional basis while completing required immunizations.",
  statement = "In {location}, {value} school students were provisionally enrolled.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$rate_incomplete <- list(
  short_name = "Incomplete immunization rate",
  long_name = "Proportion of students with incomplete immunizations",
  category = "immunization",
  short_description = "Proportion of assessed school students not up to date and not exempt.",
  long_description = "Share of assessed school students who have not completed the required immunizations and do not hold an exemption.",
  statement = "In {location}, {value} of school students had incomplete immunizations.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$N_incomplete <- list(
  short_name = "Incomplete students (count)",
  long_name = "Number of students with incomplete immunizations",
  category = "immunization",
  short_description = "Number of assessed school students not up to date and not exempt.",
  long_description = "Count of assessed school students who have not completed the required immunizations and do not hold an exemption.",
  statement = "In {location}, {value} school students had incomplete immunizations.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$rate_missing <- list(
  short_name = "Missing documentation rate",
  long_name = "Proportion of students with no immunization record on file",
  category = "immunization",
  short_description = "Proportion of assessed school students with no immunization documentation on file.",
  long_description = "Share of assessed school students for whom no immunization record was on file at the time of the survey.",
  statement = "In {location}, {value} of school students had no immunization record on file.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_90_day <- list(
  short_name = "90-day provisional rate",
  long_name = "Proportion of students in the 90-day provisional period",
  category = "immunization",
  short_description = "Proportion of students within the 90-day grace period to submit immunization documentation.",
  long_description = "Share of students within the 90-day provisional period allowed to submit immunization documentation after enrolling.",
  statement = "In {location}, {value} of school students were in the 90-day provisional period.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_pme <- list(
  short_name = "Permanent medical exemption rate",
  long_name = "Proportion of students with a permanent medical exemption",
  category = "immunization",
  short_description = "Proportion of students with a permanent medical exemption from immunization requirements.",
  long_description = "California kindergarten category: share of students granted a permanent medical exemption (PME) filed through CAIR-ME.",
  statement = "In {location}, {value} of school students had a permanent medical exemption.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_other_lacking <- list(
  short_name = "Other / not up to date rate",
  long_name = "Proportion of students lacking one or more required immunizations for other reasons",
  category = "immunization",
  short_description = "Proportion of students lacking one or more required immunizations for reasons other than exemption, conditional, or overdue status.",
  long_description = "School-entry assessment residual category: share of students not up to date and not otherwise classified as exempt, conditional, or overdue.",
  statement = "In {location}, {value} of school students lacked one or more required immunizations for other reasons.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_overdue <- list(
  short_name = "Overdue rate",
  long_name = "Proportion of students overdue for one or more required immunizations",
  category = "immunization",
  short_description = "Proportion of students past due for one or more required immunizations and not exempt or conditional.",
  long_description = "School-entry assessment category: share of students who are overdue for one or more required immunizations and are not otherwise exempt or conditionally enrolled.",
  statement = "In {location}, {value} of school students were overdue for one or more required immunizations.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")

# state-specific exemption categories
MEASURES$rate_religious_exempt <- list(
  short_name = "Religious exemption rate",
  long_name = "Proportion of students with a religious vaccination exemption",
  category = "immunization",
  short_description = "Proportion of assessed school students with a religious exemption from one or more required vaccines.",
  long_description = "Share of assessed school students holding a religious exemption. Reported by states that separate religious exemptions from other non-medical (philosophical or personal-belief) exemptions.",
  statement = "In {location}, {value} of school students had a religious vaccination exemption.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$N_religious_exempt <- list(
  short_name = "Religious exemptions (count)",
  long_name = "Number of students with a religious vaccination exemption",
  category = "immunization",
  short_description = "Number of assessed school students with a religious exemption from one or more required vaccines.",
  long_description = "Count of assessed school students holding a religious exemption. Reported by states that separate religious exemptions from other non-medical exemptions.",
  statement = "In {location}, {value} school students had a religious vaccination exemption.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$rate_religious_membership_exempt <- list(
  short_name = "Religious membership exemption rate",
  long_name = "Proportion of students with a religious membership exemption",
  category = "immunization",
  short_description = "Proportion of students exempt as members of a religious body opposed to immunization.",
  long_description = "Washington-specific category: share of students exempt as members of a religious body or church whose teachings are contrary to immunization, distinct from a personal religious exemption.",
  statement = "In {location}, {value} of school students had a religious membership exemption.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$N_religious_membership_exempt <- list(
  short_name = "Religious membership exemptions (count)",
  long_name = "Number of students with a religious membership exemption",
  category = "immunization",
  short_description = "Number of students exempt as members of a religious body opposed to immunization.",
  long_description = "Washington-specific category: count of students exempt as members of a religious body or church whose teachings are contrary to immunization, distinct from a personal religious exemption.",
  statement = "In {location}, {value} school students had a religious membership exemption.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$rate_partial_medical_exempt_utd <- list(
  short_name = "Partial medical exemption, up to date (rate)",
  long_name = "Proportion of students with a partial medical exemption who are otherwise up to date",
  category = "immunization",
  short_description = "Proportion of students with a medical exemption from some required vaccines who are up to date on the rest.",
  long_description = "Alabama category: share of students with a medical exemption from some but not all required vaccines who are up to date on the remaining required vaccines.",
  statement = "In {location}, {value} of school students had a partial medical exemption and were up to date on the rest.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$N_partial_medical_exempt_utd <- list(
  short_name = "Partial medical exemption, up to date (count)",
  long_name = "Number of students with a partial medical exemption who are otherwise up to date",
  category = "immunization",
  short_description = "Number of students with a medical exemption from some required vaccines who are up to date on the rest.",
  long_description = "Alabama category: count of students with a medical exemption from some but not all required vaccines who are up to date on the remaining required vaccines.",
  statement = "In {location}, {value} school students had a partial medical exemption and were up to date on the rest.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$rate_partial_religious_exempt_utd <- list(
  short_name = "Partial religious exemption, up to date (rate)",
  long_name = "Proportion of students with a partial religious exemption who are otherwise up to date",
  category = "immunization",
  short_description = "Proportion of students with a religious exemption from some required vaccines who are up to date on the rest.",
  long_description = "Alabama category: share of students with a religious exemption from some but not all required vaccines who are up to date on the remaining required vaccines.",
  statement = "In {location}, {value} of school students had a partial religious exemption and were up to date on the rest.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$N_partial_religious_exempt_utd <- list(
  short_name = "Partial religious exemption, up to date (count)",
  long_name = "Number of students with a partial religious exemption who are otherwise up to date",
  category = "immunization",
  short_description = "Number of students with a religious exemption from some required vaccines who are up to date on the rest.",
  long_description = "Alabama category: count of students with a religious exemption from some but not all required vaccines who are up to date on the remaining required vaccines.",
  statement = "In {location}, {value} school students had a partial religious exemption and were up to date on the rest.",
  measure_type = "Count", unit = "Students", time_resolution = "Year")
MEASURES$rate_varicella_disease_history <- list(
  short_name = "Varicella disease-history rate",
  long_name = "Proportion of students with a history of varicella disease",
  category = "immunization",
  short_description = "Proportion of students with a documented history of chickenpox accepted as evidence of immunity.",
  long_description = "Share of students with a documented history of varicella (chickenpox) disease, accepted in lieu of vaccination as evidence of immunity.",
  statement = "In {location}, {value} of school students had a documented history of varicella disease.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_mmr_exempt <- list(
  short_name = "MMR exemption rate",
  long_name = "Proportion of kindergarten students exempt from MMR vaccination",
  category = "immunization",
  short_description = "Proportion of kindergarten students with an exemption from the MMR vaccination requirement.",
  long_description = "Ohio publishes kindergarten MMR data as an exemption rate: the share of kindergarten students with a medical or non-medical exemption from the MMR requirement.",
  statement = "In {location}, {value} of kindergarten students were exempt from MMR vaccination.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_completely_immunized <- list(
  short_name = "Completely immunized rate",
  long_name = "Proportion of students completely immunized",
  category = "immunization",
  short_description = "Proportion of students at the school reported as completely immunized for all required vaccines.",
  long_description = "Share of enrolled students reported as completely immunized (all required vaccines) in the New York State School Immunization Survey. Reported at the individual-school level.",
  statement = "In {location}, {value} of students were completely immunized.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_medical_exemptions <- MEASURES$rate_medical_exempt  # NY uses the plural column name

# Maine exemption categories (rate_exempt_<type>)
MEASURES$rate_exempt_total <- list(
  short_name = "Total exemption rate",
  long_name = "Proportion of students with any vaccination exemption",
  category = "immunization",
  short_description = "Proportion of assessed school students with any exemption (medical or non-medical) from vaccination requirements.",
  long_description = "Share of assessed school students with at least one vaccination exemption of any type, as reported in the Maine School Vaccination Rates workbooks.",
  statement = "In {location}, {value} of school students had a vaccination exemption of any kind.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_exempt_medical <- list(
  short_name = "Medical exemption rate",
  long_name = "Proportion of students with a medical vaccination exemption",
  category = "immunization",
  short_description = "Proportion of assessed school students with a medical exemption from one or more required vaccines.",
  long_description = "Share of assessed school students with a physician-certified medical exemption from one or more required vaccines.",
  statement = "In {location}, {value} of school students had a medical vaccination exemption.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_exempt_religious <- list(
  short_name = "Religious exemption rate",
  long_name = "Proportion of students with a religious vaccination exemption",
  category = "immunization",
  short_description = "Proportion of assessed school students with a religious exemption from one or more required vaccines.",
  long_description = "Share of assessed school students holding a religious exemption from one or more required vaccines.",
  statement = "In {location}, {value} of school students had a religious vaccination exemption.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_exempt_philosophical <- list(
  short_name = "Philosophical exemption rate",
  long_name = "Proportion of students with a philosophical vaccination exemption",
  category = "immunization",
  short_description = "Proportion of assessed school students with a philosophical (personal-belief) exemption from one or more required vaccines.",
  long_description = "Share of assessed school students holding a philosophical or personal-belief exemption from one or more required vaccines.",
  statement = "In {location}, {value} of school students had a philosophical vaccination exemption.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")

# antigen-specific exemption rates -- Colorado (rate_<antigen>_<type>_exempt)
antigen_exempt <- function(label, type, statement_label = label) {
  tword <- if (type == "medical") "medical" else "non-medical"
  list(
    short_name = paste0(label, " ", tword, " exemption rate"),
    long_name = paste0("Proportion of students with a ", tword, " exemption from ", label, " vaccination"),
    category = "immunization",
    short_description = paste0("Proportion of assessed school students with a ", tword,
      " exemption from the ", label, " vaccination requirement."),
    long_description = paste0("Share of assessed school students holding a ", tword,
      " exemption from the ", label, " vaccine specifically. Reported by states that break exemptions out by antigen."),
    statement = paste0("In {location}, {value} of school students had a ", tword,
      " exemption from ", statement_label, " vaccination."),
    measure_type = "Rate", unit = "Rate", time_resolution = "Year")
}
co_antigens <- list(hep_b = "hepatitis B", covid = "COVID-19", polio = "polio",
  hib = "Hib", varicella = "varicella", pcv = "pneumococcal (PCV)",
  mmr = "MMR", dtap = "DTaP", tdap = "Tdap")
for (a in names(co_antigens)) {
  for (ty in c("medical", "nonmedical")) {
    MEASURES[[paste0("rate_", a, "_", ty, "_exempt")]] <- antigen_exempt(co_antigens[[a]], ty)
  }
}
# antigen-specific exemption rates -- Minnesota (rate_<antigen>_<type>, no suffix)
mn_antigens <- list(dtap = "DTaP", polio = "polio", mmr = "MMR",
  hep_b = "hepatitis B", varicella = "varicella")
for (a in names(mn_antigens)) {
  for (ty in c("medical", "nonmedical")) {
    MEASURES[[paste0("rate_", a, "_", ty)]] <- antigen_exempt(mn_antigens[[a]], ty)
  }
}

# New York per-disease "immunized" columns
ny_immunized <- function(label) list(
  short_name = paste0(label, " immunization"),
  long_name = paste0("Proportion of students immunized against ", label),
  category = "immunization",
  short_description = paste0("Proportion of students at the school reported as immunized against ", label, "."),
  long_description = paste0("Share of enrolled students reported as adequately immunized against ", label,
    " in the New York State School Immunization Survey. Reported at the individual-school level."),
  statement = paste0("In {location}, {value} of students were immunized against ", label, "."),
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
ny_diseases <- list(polio = "polio", measles = "measles", mumps = "mumps",
  rubella = "rubella", diphtheria = "diphtheria", hepatitis_b = "hepatitis B",
  varicella = "varicella", tdap = "Tdap", meningococcal = "meningococcal disease")
for (a in names(ny_diseases)) {
  MEASURES[[paste0("rate_immunized_", a)]] <- ny_immunized(ny_diseases[[a]])
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
  MEASURES[[paste0("rate_entrants_vax_", a)]] <- list(
    short_name = paste0(lab, " coverage (7th grade)"),
    long_name = paste0("Proportion of 7th-grade students up to date for ", lab, " vaccination"),
    category = "immunization",
    short_description = paste0("Proportion of 7th-grade students up to date for ", lab, " vaccination."),
    long_description = paste0("Share of 7th-grade students reported as up to date for ", lab,
      " vaccination in the California school immunization data, enrollment-weighted to the county."),
    statement = paste0("In {location}, {value} of 7th-grade students were up to date for ", lab, " vaccination."),
    measure_type = "Rate", unit = "Rate", time_resolution = "Year")
  MEASURES[[paste0("rate_conditional_", a)]] <- list(
    short_name = paste0(lab, " conditional rate (7th grade)"),
    long_name = paste0("Proportion of 7th-grade students conditionally enrolled for ", lab),
    category = "immunization",
    short_description = paste0("Proportion of 7th-grade students conditionally enrolled while completing ", lab, " requirements."),
    long_description = paste0("Share of 7th-grade students conditionally enrolled with respect to the ", lab, " requirement (in process, not overdue)."),
    statement = paste0("In {location}, {value} of 7th-grade students were conditionally enrolled for ", lab, "."),
    measure_type = "Rate", unit = "Rate", time_resolution = "Year")
  MEASURES[[paste0("rate_pme_", a)]] <- list(
    short_name = paste0(lab, " permanent medical exemption rate (7th grade)"),
    long_name = paste0("Proportion of 7th-grade students with a permanent medical exemption for ", lab),
    category = "immunization",
    short_description = paste0("Proportion of 7th-grade students with a permanent medical exemption from the ", lab, " requirement."),
    long_description = paste0("Share of 7th-grade students granted a permanent medical exemption (PME) from the ", lab, " requirement."),
    statement = paste0("In {location}, {value} of 7th-grade students had a permanent medical exemption for ", lab, "."),
    measure_type = "Rate", unit = "Rate", time_resolution = "Year")
  MEASURES[[paste0("rate_other_lacking_", a)]] <- list(
    short_name = paste0(lab, " other / not up to date rate (7th grade)"),
    long_name = paste0("Proportion of 7th-grade students lacking ", lab, " for other reasons"),
    category = "immunization",
    short_description = paste0("Proportion of 7th-grade students not up to date for ", lab, " for reasons other than exemption, conditional, or overdue status."),
    long_description = paste0("Residual category: share of 7th-grade students not up to date for ", lab, " and not otherwise classified."),
    statement = paste0("In {location}, {value} of 7th-grade students lacked ", lab, " for other reasons."),
    measure_type = "Rate", unit = "Rate", time_resolution = "Year")
  MEASURES[[paste0("rate_overdue_", a)]] <- list(
    short_name = paste0(lab, " overdue rate (7th grade)"),
    long_name = paste0("Proportion of 7th-grade students overdue for ", lab),
    category = "immunization",
    short_description = paste0("Proportion of 7th-grade students overdue for the ", lab, " vaccination and not exempt or conditional."),
    long_description = paste0("Share of 7th-grade students past due for ", lab, " vaccination and not otherwise exempt or conditionally enrolled."),
    statement = paste0("In {location}, {value} of 7th-grade students were overdue for ", lab, "."),
    measure_type = "Rate", unit = "Rate", time_resolution = "Year")
}

# ---- antigen-specific exemption rates, any reason and religious -------------
# ID publishes a per-series exemption total (rate_<antigen>_exempt); MO adds a
# medical/religious split; RI publishes religious only. None of the three
# publishes coverage, so these are the only measures those states carry.
antigen_exempt_any <- function(label, statement_label = label) list(
  short_name = paste0(label, " exemption rate"),
  long_name = paste0("Proportion of students exempt from ", label, " vaccination"),
  category = "immunization",
  short_description = paste0("Proportion of assessed school students with any exemption from the ",
    label, " vaccination requirement."),
  long_description = paste0("Share of assessed school students holding an exemption of any kind ",
    "from the ", label, " series. This is an exemption rate, not a coverage rate: a student ",
    "without an exemption is not necessarily vaccinated, and an exempt student may still have ",
    "received the vaccine, so it cannot be subtracted from one to obtain coverage."),
  statement = paste0("In {location}, {value} of school students were exempt from ",
    statement_label, " vaccination."),
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")

antigen_exempt_religious <- function(label, statement_label = label) list(
  short_name = paste0(label, " religious exemption rate"),
  long_name = paste0("Proportion of students with a religious exemption from ", label, " vaccination"),
  category = "immunization",
  short_description = paste0("Proportion of assessed school students with a religious exemption ",
    "from the ", label, " vaccination requirement."),
  long_description = paste0("Share of assessed school students holding a religious exemption from ",
    "the ", label, " series specifically. An exemption rate, not a coverage rate."),
  statement = paste0("In {location}, {value} of school students had a religious exemption from ",
    statement_label, " vaccination."),
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")

exempt_antigens <- list(dtap = "DTaP", polio = "polio", mmr = "MMR",
  hep_a = "hepatitis A", hep_b = "hepatitis B", varicella = "varicella",
  menacwy = "meningococcal (MenACWY)", tdap = "Tdap", flu = "influenza")
for (a in names(exempt_antigens)) {
  lab <- exempt_antigens[[a]]
  MEASURES[[paste0("rate_", a, "_exempt")]] <- antigen_exempt_any(lab)
  MEASURES[[paste0("rate_", a, "_religious_exempt")]] <- antigen_exempt_religious(lab)
  MEASURES[[paste0("rate_", a, "_medical_exempt")]] <- antigen_exempt(lab, "medical")
}

# ---- Illinois: every measure per antigen (rate + count) ---------------------
# ISBE publishes one sheet per vaccine (Polio, DTaP, Tdap, Measles, Mumps,
# Rubella, Hepatitis B, Hib, Varicella, Pneumococcal, Meningococcal). The
# ingest keeps a single shared N_enrolled (already covered by the generic
# entry above) rather than a per-vaccine denominator, and does not carry a
# per-vaccine coverage COUNT -- only rate_<vax> -- so this block fills just
# the antigens and the per-reason exemption COUNTS (split by religious/
# medical, rather than WV's one combined N_<vax>_exempt) that no other
# state's columns needed yet. `if (is.null(...))` keeps the wording already
# declared above for antigens other states also report (DTaP, polio, Tdap,
# hepatitis B, varicella, MenACWY, Hib, PCV).
m_vax_exempt_reason <- function(short_name, long_name, reason) {
  rword <- if (reason == "medical") "medical" else "religious"
  list(
    short_name = paste0(short_name, " ", rword, " exemptions (count)"),
    long_name = paste0("Number of students with a ", rword, " exemption from ",
                       long_name, " vaccination"),
    category = "immunization",
    short_description = paste0("Number of assessed school students with a ", rword,
      " exemption from the ", long_name, " vaccination requirement."),
    long_description = paste0("Count of assessed school students holding a ", rword,
      " exemption from the ", long_name, " vaccine specifically. Reported by ",
      "states that break exemption counts out by antigen and by reason."),
    statement = paste0("In {location}, {value} school students had a ", rword,
      " exemption from ", long_name, " vaccination."),
    measure_type = "Count", unit = "Students", time_resolution = "Year")
}
il_antigens <- list(
  polio = "polio", dtap = "diphtheria-tetanus-pertussis (DTaP)",
  tdap = "tetanus-diphtheria-pertussis booster (Tdap)",
  measles = "measles", mumps = "mumps", rubella = "rubella",
  hep_b = "hepatitis B", hib = "Haemophilus influenzae type b (Hib)",
  varicella = "varicella (chickenpox)",
  pcv = "pneumococcal conjugate (PCV)",
  menacwy = "meningococcal conjugate (MenACWY)")
il_short <- list(
  polio = "Polio", dtap = "DTaP", tdap = "Tdap", measles = "Measles",
  mumps = "Mumps", rubella = "Rubella", hep_b = "Hepatitis B", hib = "Hib",
  varicella = "Varicella", pcv = "Pneumococcal (PCV)", menacwy = "MenACWY")
for (a in names(il_antigens)) {
  lab <- il_antigens[[a]]
  short <- il_short[[a]]
  if (is.null(MEASURES[[paste0("rate_", a)]])) {
    MEASURES[[paste0("rate_", a)]] <- m_coverage(short, lab, short)
  }
  if (is.null(MEASURES[[paste0("rate_", a, "_religious_exempt")]])) {
    MEASURES[[paste0("rate_", a, "_religious_exempt")]] <- antigen_exempt_religious(lab)
  }
  if (is.null(MEASURES[[paste0("rate_", a, "_medical_exempt")]])) {
    MEASURES[[paste0("rate_", a, "_medical_exempt")]] <- antigen_exempt(lab, "medical")
  }
  MEASURES[[paste0("N_", a, "_religious_exempt")]] <-
    m_vax_exempt_reason(short, lab, "religious")
  MEASURES[[paste0("N_", a, "_medical_exempt")]] <-
    m_vax_exempt_reason(short, lab, "medical")
}

# ---- antigen-specific non-compliance -----------------------------------------
# Connecticut publishes, per antigen, the share of pupils who are neither up to
# date nor exempt -- its "percentage_non_compliant". It is the residual left
# after coverage and both exemption categories, so it is not one minus coverage:
# an exempt pupil is not vaccinated but is compliant.
antigen_non_compliant <- function(label, statement_label = label) list(
  short_name = paste0(label, " non-compliance rate"),
  long_name = paste0("Proportion of students neither up to date for nor exempt from ", label, " vaccination"),
  category = "immunization",
  short_description = paste0("Proportion of assessed school students who are neither up to date for ",
    label, " vaccination nor hold an exemption from it."),
  long_description = paste0("Share of assessed school students out of compliance with the ", label,
    " requirement: the residual left after up-to-date pupils and pupils holding a medical or ",
    "religious exemption. It is not one minus coverage, because an exempt pupil counts as ",
    "compliant while unvaccinated."),
  statement = paste0("In {location}, {value} of school students were out of compliance with the ",
    statement_label, " vaccination requirement."),
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
for (a in names(exempt_antigens)) {
  MEASURES[[paste0("rate_", a, "_non_compliant")]] <-
    antigen_non_compliant(exempt_antigens[[a]])
}

# ---- Illinois: non-compliant headcount and the source's own "School
# ---- Compliance %" -----------------------------------------------------------
# ISBE's per-antigen sheets carry an unduplicated non-compliant headcount
# (rate_<a>_non_compliant above is the share; no state's columns needed the
# count behind it until now) and their own "School Compliance %", published
# directly rather than derived here. The two do not reconcile arithmetically
# -- Adams County Polio 2024 is 99.96% school compliance against a
# non-compliant headcount of 12 of 10,513 enrolled, i.e. 99.89% -- so
# rate_<a>_school_compliance is its own measure rather than an alias for
# rate_<a>_non_compliant or for coverage.
m_vax_non_compliant_count <- function(short_name, long_name) {
  list(
    short_name = paste0(short_name, " non-compliant (count)"),
    long_name = paste0("Number of students neither up to date for nor exempt from ",
                       long_name, " vaccination"),
    category = "immunization",
    short_description = paste0("Number of assessed school students who are neither up ",
      "to date for ", long_name, " vaccination nor hold an exemption from it."),
    long_description = paste0("Unduplicated count of assessed school students out of ",
      "compliance with the ", long_name, " requirement: the residual left after ",
      "up-to-date pupils and pupils holding a medical or religious exemption. Not ",
      "one minus the coverage count, because an exempt pupil counts as compliant ",
      "while unvaccinated."),
    statement = paste0("In {location}, {value} school students were out of compliance ",
      "with the ", long_name, " vaccination requirement."),
    measure_type = "Count", unit = "Students", time_resolution = "Year")
}
m_vax_school_compliance <- function(short_name, long_name) {
  list(
    short_name = paste0(short_name, " school compliance rate"),
    long_name = paste0("Proportion of students in school compliance for ", long_name,
                       " vaccination"),
    category = "immunization",
    short_description = paste0("Proportion of assessed school students the source ",
      "classifies as in compliance for the ", long_name, " vaccination requirement."),
    long_description = paste0("Directly published by the source as its own \"School ",
      "Compliance %\" per vaccine, alongside coverage and a non-compliant headcount; ",
      "the three do not always reconcile arithmetically, so this is kept as its own ",
      "measure rather than treated as equivalent to coverage or to one minus the ",
      "non-compliance rate."),
    statement = paste0("In {location}, {value} of school students were in school ",
      "compliance for ", long_name, " vaccination."),
    measure_type = "Rate", unit = "Rate", time_resolution = "Year")
}
for (a in names(il_antigens)) {
  lab <- il_antigens[[a]]
  short <- il_short[[a]]
  if (is.null(MEASURES[[paste0("rate_", a, "_non_compliant")]])) {
    MEASURES[[paste0("rate_", a, "_non_compliant")]] <- antigen_non_compliant(lab)
  }
  MEASURES[[paste0("N_", a, "_non_compliant")]] <- m_vax_non_compliant_count(short, lab)
  MEASURES[[paste0("rate_", a, "_school_compliance")]] <- m_vax_school_compliance(short, lab)
}

MEASURES$rate_all_required_non_compliant <- list(
  short_name = "All-required non-compliance rate",
  long_name = "Proportion of students neither up to date for nor exempt from the required schedule",
  category = "immunization",
  short_description = "Proportion of assessed school students who are neither up to date across the required vaccination schedule nor hold an exemption from it.",
  long_description = "Share of assessed school students out of compliance with the state's full required schedule: the residual left after up-to-date pupils and pupils holding a medical or religious exemption. It is not one minus coverage, because an exempt pupil counts as compliant while unvaccinated.",
  statement = "In {location}, {value} of school students were out of compliance with the required vaccination schedule.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_flu <- m_coverage("Influenza", "influenza", "influenza")

# ---- all-required-schedule exemption rates -----------------------------------
# CT reports exemptions against its whole required schedule as well as per
# antigen, under the same "All" series that gives rate_all_required coverage.
all_required_exempt <- function(tword) list(
  short_name = paste0("All-required ", tword, " exemption rate"),
  long_name = paste0("Proportion of students with a ", tword,
                     " exemption from the required schedule"),
  category = "immunization",
  short_description = paste0("Proportion of assessed school students with a ", tword,
    " exemption from one or more vaccines in the required schedule."),
  long_description = paste0("Share of assessed school students holding a ", tword,
    " exemption measured against the full required schedule rather than a single antigen. ",
    "An exemption rate, not a coverage rate: an exempt pupil may still have been vaccinated."),
  statement = paste0("In {location}, {value} of school students had a ", tword,
    " exemption from the required vaccination schedule."),
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_all_required_religious_exempt <- all_required_exempt("religious")
MEASURES$rate_all_required_medical_exempt <- all_required_exempt("medical")
MEASURES$rate_all_required_full_exempt <- list(
  short_name = "All-required total exemption rate",
  long_name = "Proportion of students with any exemption from the required schedule",
  category = "immunization",
  short_description = "Proportion of assessed school students with an exemption of any kind from one or more vaccines in the required schedule.",
  long_description = "Share of assessed school students holding an exemption of any kind - medical or religious - measured against the full required schedule rather than a single antigen. An exemption rate, not a coverage rate: an exempt pupil may still have been vaccinated.",
  statement = "In {location}, {value} of school students had an exemption from the required vaccination schedule.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")

# ---- AL exemption components -------------------------------------------------
# ADPH distinguishes a full exemption from a partial one where the student is
# otherwise up to date, and reports medical and religious separately.
MEASURES$rate_full_medical_exempt <- list(
  short_name = "Full medical exemption rate",
  long_name = "Proportion of students with a full medical exemption",
  category = "immunization",
  short_description = "Proportion of assessed school students with a full medical exemption from all required vaccines.",
  long_description = "Share of assessed school students holding a full medical exemption, as distinct from a partial medical exemption where the student is otherwise up to date.",
  statement = "In {location}, {value} of school students had a full medical exemption.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_full_religious_exempt <- list(
  short_name = "Full religious exemption rate",
  long_name = "Proportion of students with a full religious exemption",
  category = "immunization",
  short_description = "Proportion of assessed school students with a full religious exemption from all required vaccines.",
  long_description = "Share of assessed school students holding a full religious exemption, as distinct from a partial religious exemption where the student is otherwise up to date.",
  statement = "In {location}, {value} of school students had a full religious exemption.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$N_full_medical_exempt <- m_count("Full medical exemptions", "a full medical exemption")
MEASURES$N_full_religious_exempt <- m_count("Full religious exemptions", "a full religious exemption")

# ---- status categories not already covered ----------------------------------
MEASURES$rate_utd <- list(
  short_name = "Up-to-date rate",
  long_name = "Proportion of students up to date for all required vaccines",
  category = "immunization",
  short_description = "Proportion of assessed school students up to date for every vaccine the state requires.",
  long_description = "Share of assessed school students recorded as up to date across the full required schedule, rather than for a single antigen.",
  statement = "In {location}, {value} of school students were up to date for all required vaccines.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_not_utd <- list(
  short_name = "Not-up-to-date rate",
  long_name = "Proportion of students not up to date for all required vaccines",
  category = "immunization",
  short_description = "Proportion of assessed school students not up to date for every vaccine the state requires.",
  long_description = "Share of assessed school students not recorded as up to date across the full required schedule, excluding those conditionally enrolled.",
  statement = "In {location}, {value} of school students were not up to date for all required vaccines.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_no_record <- list(
  short_name = "No immunization record rate",
  long_name = "Proportion of students with no immunization record on file",
  category = "immunization",
  short_description = "Proportion of assessed school students for whom no immunization record was on file.",
  long_description = "Share of assessed school students with no immunization record at all, which is distinct from having an incomplete record or an exemption.",
  statement = "In {location}, {value} of school students had no immunization record on file.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_fully_immunized <- list(
  short_name = "Fully immunized rate",
  long_name = "Proportion of students fully immunized for all required vaccines",
  category = "immunization",
  short_description = "Proportion of assessed school students recorded as fully immunized for every required vaccine.",
  long_description = "Share of assessed school students the state reports as fully immunized across the required schedule, as opposed to provisionally admitted or exempt.",
  statement = "In {location}, {value} of school students were fully immunized.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")
MEASURES$rate_any_exempt <- list(
  short_name = "Any exemption rate",
  long_name = "Proportion of students with an exemption of any kind",
  category = "immunization",
  short_description = "Proportion of assessed school students with an exemption of any kind from immunization requirements.",
  long_description = "Share of assessed school students holding an exemption of any kind, medical or non-medical, as published by the state without a breakdown by reason.",
  statement = "In {location}, {value} of school students had an exemption of any kind.",
  measure_type = "Rate", unit = "Rate", time_resolution = "Year")

# ---- denominators and counts not already covered ----------------------------
MEASURES$N_assessed <- m_denom(
  "Students assessed",
  "Number of students assessed in the immunization survey",
  "Number of school students whose immunization records were assessed.",
  paste0("Count of students whose records were reviewed for this report. This is the survey ",
    "denominator and is not necessarily the same as total enrolment, since not every enrolled ",
    "student is always assessed."),
  "In {location}, {value} school students were assessed.")
MEASURES$N_total_audited <- m_denom(
  "Records audited",
  "Number of student records audited",
  "Number of student immunization records audited.",
  "Count of student records the state audited for this report, used as the denominator for the audited counts alongside it.",
  "In {location}, {value} student records were audited.")
MEASURES$total_pop_4_18 <- m_denom(
  "Population aged 4-18",
  "Census population aged 4 to 18",
  "Census population aged 4 to 18 in the county.",
  paste0("Population aged 4 to 18 from census estimates, used as an approximate denominator ",
    "where the state publishes exemption counts without an enrolment figure. It is a resident ",
    "population, not an enrolment count, so rates derived from it are approximate."),
  "In {location}, the population aged 4 to 18 was {value}.")
MEASURES$N_exempt <- m_count("Exemptions", "an exemption")
MEASURES$N_mmr_k <- m_count("Kindergarten MMR vaccinated", "a record of MMR vaccination in kindergarten")
MEASURES$N_men_6th <- m_count("6th grade meningococcal vaccinated", "a record of meningococcal vaccination in 6th grade")
MEASURES$N_schools <- m_denom(
  "Schools in county",
  "Number of schools reporting in the county",
  "Number of schools in the county that submitted a report.",
  "Count of schools in the county that submitted a report, whether or not their figures were suppressed.",
  "In {location}, {value} schools submitted a report.")
MEASURES$N_schools_reported <- m_denom(
  "Schools with unsuppressed figures",
  "Number of schools whose figures were not suppressed",
  "Number of schools in the county whose figures were published rather than suppressed.",
  paste0("Count of schools actually contributing to the county rate. Where this is far below ",
    "N_schools the county figure rests on a small and non-random subset of its schools -- the ",
    "unsuppressed ones -- and should not be read as a county-wide rate."),
  "In {location}, {value} schools had unsuppressed figures.")

# ---- per-measure censoring flags ---------------------------------------------
# flag_<x> says what the source printed for rate_<x> on this row, which the
# row-level suppressed_flag below cannot: it only ever said that SOMETHING on
# the row was censored, not which measure or why. Generated from the rate_
# dictionary so a
# new rate gets its flag for free; only the flags a state's standard file
# actually carries are emitted into its measure_info.json.
#
# Must stay after every rate_ entry is defined -- the loop reads names(MEASURES).
CENSOR_FLAG_VALUES <- paste0(
  "BLANK means the source printed a figure and nothing is censored -- only the ",
  "exceptions carry text. \"suppressed\" means the source withheld the cell to ",
  "protect a small count (an asterisk run: \"*\", \"**\", \"***\"). ",
  "\"missing\" means it was not reported at all (\"N/A\", \"--\", \"NR\"). ",
  "\"top_coded\" means the source printed an upper bound such as ",
  "\">95%\" instead of a figure, so the true value lies ABOVE the bound; the ",
  "rate column holds the bound the source printed, not a substitute invented ",
  "for it, so it must be read as interval-censored rather than as a ",
  "measurement. \"bottom_coded\" is the same the other way (\"<5\"). ",
  "\"suppressed\" and \"missing\" both leave the rate empty but ",
  "are not the same thing: a suppressed cell has a real measurement behind it ",
  "that the state chose not to print, whereas a missing one was never ",
  "collected.")

for (rc in grep("^rate_", names(MEASURES), value = TRUE)) {
  lab <- MEASURES[[rc]]$short_name
  if (is.null(lab)) lab <- rc
  MEASURES[[sub("^rate_", "flag_", rc)]] <- list(
    short_name = paste0(lab, " - censoring flag"),
    long_name = paste0("Why ", rc, " carries no value on this row"),
    category = "immunization",
    short_description = paste0("Whether ", rc, " was published as a number, ",
      "suppressed, not reported, or censored at a bound."),
    long_description = paste0("Per-measure companion to ", rc, ". ",
      CENSOR_FLAG_VALUES),
    statement = paste0("In {location}, ", rc, " was {value}."),
    measure_type = "Category", unit = "Category", time_resolution = "Year")
}

# ---- censoring / suppression -------------------------------------------------
MEASURES$suppressed_flag <- list(
  short_name = "Suppressed value flag",
  long_name = "Indicator that at least one rate on this row was censored at source",
  category = "immunization",
  short_description = "1 if at least one rate on this row was suppressed or top-coded by the source.",
  long_description = paste0("Set to 1 where the source published a censoring marker instead of a ",
    "number for at least one measure on the row. Undirected markers leave the value missing. ",
    "Where a bound is given, see censor_direction for which way it runs. This is a row-level ",
    "rollup and cannot say WHICH measure was censored; where the state's file carries ",
    "flag_<measure> columns, use those instead."),
  statement = "In {location}, at least one reported rate was censored at source.",
  measure_type = "Count", unit = "Flag", time_resolution = "Year")
MEASURES$censor_direction <- list(
  short_name = "Censoring direction",
  long_name = "Direction in which a censored value on this row is bounded",
  category = "immunization",
  short_description = "Which way a censored value is bounded: left, right, undirected, or a combination.",
  long_description = paste0("\"left\" means the source printed a less-than marker, so the true ",
    "value lies BELOW the stated threshold (a suppressed small cell such as \"<5\"). \"right\" ",
    "means a greater-than marker, so the true value lies ABOVE it (a top code such as \">95%\"). ",
    "\"undirected\" means the value was withheld with no bound stated. Combinations are joined ",
    "with \"+\" where a row carries more than one kind. Empty where nothing was censored. ",
    "Left- and right-censoring pull an estimate in opposite directions, so they are recorded ",
    "separately rather than under a single suppression flag."),
  statement = "In {location}, censored values on this row were bounded {value}.",
  measure_type = "Category", unit = "Category", time_resolution = "Year")

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
  paste0("County-level antigen-specific immunization COVERAGE and EXEMPTION rates from CDPHE, ",
    "pulled from the Colorado Information Marketplace (Socrata 3b5w-8ggf) / CDPHE ArcGIS Open ",
    "Data, for child care/preschool, kindergarten and K-12. ",
    "COLUMN LAYOUT -- one row per county/year/grade, and three columns per vaccine, so every ",
    "column means the same thing on every row. For each of the nine vaccines (MMR, DTaP, Tdap, ",
    "polio, hepatitis B, varicella, Hib, PCV, COVID-19): rate_<vaccine> is CDPHE's \"Fully ",
    "Immunized\" figure, i.e. coverage for that vaccine, and carries the same meaning here as in ",
    "every other state in this collection; rate_<vaccine>_medical_exempt and ",
    "rate_<vaccine>_nonmedical_exempt are that vaccine's two exemption rates. Coverage and ",
    "exemptions are separate columns and must not be combined: an exempt student may still have ",
    "been vaccinated, so the exemption rate is not one minus coverage, and the three columns do ",
    "not sum to 1 -- CDPHE also publishes \"In Process\", \"Incomplete Record\" and \"No Record\", ",
    "which are not ingested. ",
    "DENOMINATORS -- N_<vaccine>_enrolled is per VACCINE, not per row: CDPHE assesses each vaccine ",
    "over the grades that require it, so in Adams County 2021/2022 the Tdap denominator is 47,061 ",
    "against 85,200 for the K-12 vaccines. There is no single N_enrolled for a row; each vaccine's ",
    "denominator is shared by its coverage and its two exemption columns. ",
    "SOURCE DUPLICATE -- Larimer County's 2022/2023 Tdap records appear twice, once with the ",
    "published figures and once with 0 throughout; the figures are kept."))

add_src("CT", "Connecticut School Immunization and Exemption Rates",
  "https://data.ct.gov/resource/8kid-pp5k",
  "Connecticut Department of Public Health", "https://portal.ct.gov/dph",
  paste0(
    "Two DPH sources on one (school year x county x grade) key. CT Open Data ",
    "(Socrata) dataset 8kid-pp5k gives, for grades Pre-K / K / 7th and ten ",
    "vaccine series, enrolment plus that series' coverage, religious and ",
    "medical exemption and non-compliance rates; a crosswalk handles the 2022+ ",
    "county-to-planning-region transition. DPH's all-grades exemption workbook ",
    "(CT Vaccine Exemptions 2017-2025) adds religious, medical and total ",
    "exemption rates across every grade, by traditional county, for 2017-18 .. ",
    "2024-25; those rows carry no enrolment and no coverage, and are the only ",
    "all-grades figures CT publishes."),
  paste0(
    "Public data published by the source agency; no access restrictions. The ",
    "all-grades workbook is a manual download with no API. Two impossible ",
    "source values are dropped to NA by the ingest, which names them: a ",
    "misplaced decimal in 2025-26 Northwest Hills 7th-grade HepA compliance, ",
    "and 2012-13 Windham Pre-K enrolment, filed with the statewide figure. The ",
    "2024-25 7th-grade statewide row is short of the sum of its planning ",
    "regions, as DPH published it."))

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
  paste0("Per-series RELIGIOUS EXEMPTION rates supplied to PopHIVE by RIDOH, for kindergarten ",
    "and 7th grade. ",
    "RELIGIOUS EXEMPTIONS ONLY -- row 1 of the workbook reads \"Religious exemption rates\", so ",
    "the coverage columns are empty, and there is no medical or any-reason figure. Do not compute ",
    "coverage as 1 minus these values: a student without an exemption is not necessarily ",
    "vaccinated, and an exempt student may still have received the vaccine. ",
    "STATE LEVEL ONLY -- Rhode Island supplies no county breakdown here, so geography is the ",
    "2-digit state FIPS (44) on every row and this source contributes nothing to county analyses. ",
    "Small cells are suppressed with a less-than marker; those are left missing with ",
    "suppressed_flag = 1 and censor_direction = \"left\"."),
  "Public data supplied by RIDOH.")

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
  paste0("County-level K-12 EXEMPTION data from the annual Iowa HHS school and child-care audit ",
    "reports. Medical and religious certificates are published in separate files, each with a ",
    "certificate count and the audited enrolment. ",
    "RATES ARE COMPUTED FROM THE COUNTS, not read from the published percentage column. Iowa's ",
    "medical-exemption rates are all below 1 percent, which used to defeat a scale check and get ",
    "the whole file multiplied by 100 -- Dickinson County 2020-21, with 27 medical certificates ",
    "against 2,707 enrolled, shipped as 100 percent instead of 1.0 percent. Dividing the ",
    "certificate count by the audited enrolment removes the question of scale entirely. ",
    "rate_full_exempt is the combined certificate count over the same denominator, rather than ",
    "the sum of the two component rates, because the two files are joined on enrolment and can ",
    "disagree about a county's audited total."))

add_src("IL", "Illinois School Immunization Data",
  "https://www.isbe.net/Pages/Health-Requirements-Student-Data.aspx",
  "Illinois State Board of Education", "https://www.isbe.net/",
  "County-level immunization and exemption data from the ISBE public-use immunization data files, published as one sheet per vaccine (Polio, DTaP, Tdap, Measles, Mumps, Rubella, Hepatitis B, Hib, Varicella, Pneumococcal, Meningococcal). Each vaccine carries its own enrollment -- Tdap and Meningococcal assess a 7th/9-12 grade cohort, the rest assess PreK-12 -- so every measure is kept per vaccine rather than collapsed into one antigen's numbers.")

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
  paste0("County-level per-series EXEMPTION rates from the Idaho DHW school immunization report ",
    "(supplied to PopHIVE as \"Yale School Exemption Data Request\"), aggregated over ",
    "kindergarten, 1st, 7th and 12th grade. ",
    "EXEMPTIONS ONLY -- this source publishes no coverage, so the coverage columns are empty and ",
    "only rate_<series>_exempt is populated. Do not compute coverage as 1 minus these values: a ",
    "student without an exemption is not necessarily vaccinated, and an exempt student may still ",
    "have received the vaccine. DHW notes that schools self-report aggregate counts and that ",
    "arithmetic errors sometimes persist."))

add_src("MO", "Missouri School Immunization Data",
  "https://health.mo.gov/living/families/schoolhealth/dashboard.php",
  "Missouri Department of Health and Senior Services", "https://health.mo.gov/",
  paste0("County-level per-series EXEMPTION rates from the Missouri DHSS school immunization ",
    "dashboard, workbook \"Exemption Rates for Kindergarteners in Missouri Schools, by Vaccine ",
    "Series\", covering kindergarten, 7th grade and K-12. Medical, religious and total are ",
    "reported for each series. ",
    "EXEMPTIONS ONLY -- this source publishes no coverage, so the coverage columns are empty. Do ",
    "not compute coverage as 1 minus these values: a student without an exemption is not ",
    "necessarily vaccinated, and an exempt student may still have received the vaccine. Medical ",
    "and religious do not always sum exactly to the reported total, since each is rounded to ",
    "three decimals at source."))

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
  paste0("School- and county-level immunization completeness, provisional, and exemption data ",
    "from the Vermont DOH school vaccination reports. ",
    "TOP CODING: the workbook prints \">95%\" rather than a figure wherever coverage exceeds 95 ",
    "percent, which is 660 of its cells. The value kept for those is the bound the workbook ",
    "printed -- 95 percent, i.e. 0.95 on the rate scale -- and nothing is substituted for it, so ",
    "no number in this file disagrees with the source. But 0.95 on a top-coded row is a LOWER ",
    "BOUND, not a measurement: the true value lies somewhere in 0.95-1.00, so averaging over ",
    "these rows understates coverage. Use flag_<measure> to exclude them or to model them as ",
    "interval-censored on [0.95, 1.00] when precision matters. ",
    "Cells marked \"*\", \"**\", \"***\" or \"N/A\" state no bound and are left missing. ",
    "PER-MEASURE FLAGS -- every rate has a flag_<measure> companion recording what the workbook ",
    "actually printed for that cell: blank (a figure, nothing censored), \"suppressed\" (an ",
    "asterisk run), \"top_coded\" (\">95%\", rate = the printed 0.95 bound) or \"missing\" ",
    "(\"N/A\"). Coverage ",
    "measures are heavily top-coded -- hepatitis B is 153 of 256 rows -- while ",
    "rate_medical_exempt is suppressed in 171 of 256. No row in this file carries \"missing\": ",
    "the workbook's only eight \"N/A\" cells are meningococcal coverage, which is not among the ",
    "measures ingested. These flags replace the row-level suppressed_flag and censor_direction, ",
    "which this source no longer emits -- they said only that something on a row was censored, ",
    "which the per-measure flags say precisely. ",
    "EXEMPTION CATEGORIES -- the workbook reports three, each kept as its own column: medical ",
    "(rate_medical_exempt), religious (rate_religious_exempt) and provisional admittance ",
    "(rate_provisional_admittance). Vermont repealed its philosophical exemption in 2016, so ",
    "religious is the only non-medical category and the harmonized rate_personal_exempt carries ",
    "the same number as rate_religious_exempt -- do not add them together. Provisional ",
    "admittance is a legal status, not an exemption; students admitted while doses are ",
    "outstanding are still counted as not up to date."),
  "Public data. Vermont suppresses small counts to protect privacy.")

add_src("WA", "Washington School Immunization Data",
  "https://doh.wa.gov/data-and-statistical-reports/washington-tracking-network-wtn/school-immunization",
  "Washington State Department of Health", "https://doh.wa.gov/",
  "County-level school immunization coverage and exemption data, including religious membership exemptions, from the WA DOH school immunization reports.")

add_src("WI", "Wisconsin School Immunization Data",
  "https://www.dhs.wisconsin.gov/library/collection/p-01892",
  "Wisconsin Department of Health Services", "https://www.dhs.wisconsin.gov/",
  paste0("School-level immunization and waiver data from the Wisconsin DHS student immunization ",
    "reports, aggregated to county here. ",
    "MAJOR CAVEAT -- these county figures are not county rates. DHS suppresses any school-level ",
    "share below 5 percent as \"<5\", which covers about 95 percent of cells: for the health ",
    "waiver, 2,980 of 3,011 schools in 2023-24, and in 38 of 72 counties every single school is ",
    "suppressed. The workbook publishes no enrolment column, so the surviving schools cannot be ",
    "weighted and the county figure is an unweighted mean over them. Because suppression removes ",
    "exactly the low values, the schools that remain are the high outliers, and every county ",
    "figure in this file rests on precisely one school. A value of 0.82 therefore describes one ",
    "small school, not the county. Use N_schools and N_schools_reported to see how thin each ",
    "figure is, and prefer a source with denominators for county-level comparison."))

add_src("AL", "Alabama School-Entry Immunization Data",
  "https://www.alabamapublichealth.gov/immunization/school-entry-survey.html",
  "Alabama Department of Public Health", "https://www.alabamapublichealth.gov/",
  paste0("County-level school-entry EXEMPTION data from the Alabama DPH school-entry survey, for ",
    "kindergarten, 7th and 9th grade. ADPH reports four categories: full medical, partial medical ",
    "where the student is otherwise up to date, full religious, and partial religious where the ",
    "student is otherwise up to date. It publishes no coverage, so the coverage columns are empty. ",
    "MAPPING ONTO THE STANDARD COLUMNS -- Alabama has no philosophical or personal-belief ",
    "category, and the standard columns are currently filled as: rate_medical_exempt = full ",
    "medical only (partial medical is excluded, and is available separately as ",
    "rate_partial_medical_exempt_utd); rate_personal_exempt = full religious; and ",
    "rate_full_exempt = full religious. The last two mean rate_full_exempt, ",
    "rate_personal_exempt and rate_full_religious_exempt all carry the same number, so an ",
    "any-reason exemption total for Alabama is NOT available in rate_full_exempt -- it excludes ",
    "medical exemptions entirely. Sum rate_full_medical_exempt and rate_full_religious_exempt for ",
    "a full-exemption total. This mapping was set by an explicit request rather than derived, so ",
    "it is recorded here rather than silently changed."))

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

add_src("AK", "Alaska Vaccination Coverage Report, Kindergarten and Adolescent Series",
  "https://health.alaska.gov/en/data-and-statistics/data-and-statistics-communicable-diseases/",
  "Alaska Department of Health, Division of Public Health", "https://health.alaska.gov/",
  paste0("Quarterly report built from VacTrAK, Alaska's immunization registry, rather than from ",
    "a school-entry survey. Two of its tables are school-relevant and ingested: the kindergarten ",
    "series (ages 5-6, grade = \"Kindergarten\": DTaP, polio, hepatitis B, MMR, varicella, ",
    "hepatitis A, and an overall up-to-date rate) and the adolescent series (ages 13-17, ",
    "grade = \"Adolescent\": Tdap, HPV, MenACWY, and an overall up-to-date rate); its 19-35-month ",
    "and adult tables are out of scope for school immunizations. Some quarters omit the ",
    "adolescent Tdap and overall rate -- VacTrAK had a forecasting error that made Tdap ",
    "unassessable for adolescents and adults that quarter, and the overall rate cannot be ",
    "computed without it -- so HPV and MenACWY are populated but rate_tdap and rate_complete are ",
    "NA for that grade/quarter. Coverage is broken out by Alaska's 7 public-health regions ",
    "(Anchorage, Gulf Coast, Interior, Mat-Su, Northern, Southeast, Southwest) plus a statewide ",
    "figure. Most regions bundle multiple boroughs/census areas and have no FIPS of their own, so ",
    "they carry geography = NA; Anchorage is coextensive with the Municipality of Anchorage and ",
    "resolves to that borough's FIPS. Each observation is dated to the school year its reporting ",
    "quarter falls within (Q1/Q2 snapshots to the school year that ended that calendar year, Q3/Q4 ",
    "to the one that started it). COVERAGE ONLY -- no exemption or enrollment counts are published ",
    "in either table."))

# ---- index/dimension columns (never emitted as measures) ---------------------
INDEX_COLS <- c(
  "time", "state", "geography", "geography_name", "county", "grade",
  "school_name", "school_id", "district", "school_type", "source_grade",
  "health_district", "school_district", "unit", "public_independent", "vaccine",
  "city",
  # How long a period a row covers. MS publishes one workbook per school year
  # and one covering three at once; multi_year marks the latter so a cumulative
  # count is not read as a single year's.
  "period_years", "multi_year"
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
