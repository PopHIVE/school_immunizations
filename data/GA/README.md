# GA

Georgia. No data yet.

The Georgia Department of Public Health publishes statewide child
immunization study reports only (`https://dph.georgia.gov/immunizations/immunization-study-reports`);
no county table or dashboard was found. The Washington Post obtained county
kindergarten coverage and exemption rates from GDPH by records request, and
PopHIVE/Ingest carries those rows in `schoolvax_washpost`. `sources.json`
records the source as request-only; `ingest.R` is a documented stub that
succeeds with no output while `raw/` is empty and stops if a file appears
without a parser.

Options: request the county table from the GDPH Immunization Program, or
rely on the Washington Post rows. See `data/DATA_SOURCES.md`.
