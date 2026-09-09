# District of Columbia
#
# No ingest yet. DC Health publishes kindergarten MMR coverage by school at
# https://dchealth.dc.gov/page/mmr-rates-school, and the Washington Post
# obtained county-level (District-wide, FIPS 11001) and school-level rates
# from DC Health by records request. dchealth.dc.gov answers HTTP 403 to
# every non-residential client tried from this environment, so the page
# cannot be fetched from CI. See sources.json for the source record and
# data/DATA_SOURCES.md for the options (fetch from a residential connection
# and commit the table to raw/, request the data from the DC Health
# Immunization Division, or rely on the Washington Post rows that
# PopHIVE/Ingest already carries).
#
# When a file is available, put it in raw/ and follow the pattern used by
# the other states: source resources/fetch.R, resources/rate_scale.R and
# resources/school_year.R, parse into the shared wide layout (time,
# geography, geography_name, type, grade, N_*, rate_*), and write with
# write_standard(). Grade and exemption column names come from the
# dictionary in scripts/generate_measure_info.R.

source("../../resources/fetch.R")

process <- dcf::dcf_process_record()
raw_files <- list.files("raw", full.names = TRUE)
if (!length(raw_files)) {
  message("DC: no raw files; source is request-only (see sources.json)")
} else {
  stop("DC: raw files are present but no parser has been written for them yet")
}
