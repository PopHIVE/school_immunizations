# DC

District of Columbia. No data yet.

DC Health publishes kindergarten MMR coverage by school at
`https://dchealth.dc.gov/page/mmr-rates-school`, and the Washington Post
obtained District-wide (FIPS 11001) and school-level rates from DC Health by
records request. dchealth.dc.gov answers HTTP 403 to every non-residential
client tried, so the page cannot be fetched from CI. `sources.json` records
the source as request-only; `ingest.R` is a documented stub that succeeds
with no output while `raw/` is empty.

Options, in order of effort: fetch the page from a residential connection
and commit the table to `raw/`; request the data from the DC Health
Immunization Division; or rely on the Washington Post rows that
PopHIVE/Ingest already carries. See `data/DATA_SOURCES.md`.
