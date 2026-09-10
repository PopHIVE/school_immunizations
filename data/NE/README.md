# NE

Nebraska. No data yet.

The Nebraska Department of Health and Human Services reports school and
child care immunization figures statewide only
(`https://dhhs.ne.gov/Pages/Licensed-Child-Care-Immunization.aspx`); no
county or health-district table or dashboard was found. The Washington Post
obtained kindergarten rates by local health district from DHHS by records
request, and PopHIVE/Ingest carries those rows in `schoolvax_washpost`.
`sources.json` records the source as request-only; `ingest.R` is a
documented stub that succeeds with no output while `raw/` is empty and stops
if a file appears without a parser.

Options: request the district or county table from the DHHS Immunization
Program, or rely on the Washington Post rows. See `data/DATA_SOURCES.md`.
