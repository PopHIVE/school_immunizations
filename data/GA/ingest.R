# Georgia
#
# No ingest yet. The Georgia Department of Public Health publishes statewide
# child immunization study reports only, at
# https://dph.georgia.gov/immunizations/immunization-study-reports; no county
# table or dashboard was found there or elsewhere on dph.georgia.gov. The
# Washington Post obtained county kindergarten coverage and exemption rates
# from GDPH by records request, and PopHIVE/Ingest already carries those rows
# (schoolvax_washpost). See sources.json for the source record and
# data/DATA_SOURCES.md for the options (request the county table from the
# GDPH Immunization Program, or rely on the Washington Post rows).
#
# When a file is available, put it in raw/ and follow the pattern used by
# the other states: source resources/fetch.R, resources/rate_scale.R,
# resources/school_year.R and resources/county_fips.R, parse into the shared
# wide layout (time, geography, geography_name, type, grade, N_*, rate_*),
# and write with write_standard(). Grade and exemption column names come
# from the dictionary in scripts/generate_measure_info.R.

source("../../resources/fetch.R")

process <- dcf::dcf_process_record()
raw_files <- list.files("raw", full.names = TRUE)
if (!length(raw_files)) {
  message("GA: no raw files; source is request-only (see sources.json)")
} else {
  stop("GA: raw files are present but no parser has been written for them yet")
}
