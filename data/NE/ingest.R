# Nebraska
#
# No ingest yet. The Nebraska Department of Health and Human Services reports
# school and child care immunization figures statewide only, at
# https://dhhs.ne.gov/Pages/Licensed-Child-Care-Immunization.aspx; no county
# or health-district table or dashboard was found. The Washington Post
# obtained kindergarten rates by local health district from DHHS by records
# request, and PopHIVE/Ingest already carries those rows (schoolvax_washpost).
# See sources.json for the source record and data/DATA_SOURCES.md for the
# options (request the district or county table from the DHHS Immunization
# Program, or rely on the Washington Post rows).
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
  message("NE: no raw files; source is request-only (see sources.json)")
} else {
  stop("NE: raw files are present but no parser has been written for them yet")
}
