# NY

New York State Department of Health, School Immunization Survey, from Health
Data NY (Socrata).

## Sources

- `btkd-y8bp`, School Immunization Survey: Beginning 2019-20 School Year.
  Refreshed by the state each year and pulled on every run into
  `raw/ny_school_immunization_survey.csv`.
- `5pme-xbs5`, School Immunization Survey: 2012-13 through 2018-19. A closed
  series, fetched once into `raw/ny_school_immunization_survey_2012_2018.csv`
  and then kept.

The older file carries a religious-exemption share (repealed in June 2019)
and lacks Tdap and meningococcal; its per-disease columns are percent points
while the newer file and both files' exemption columns are proportions. The
ingest declares the scale per column.

## Output

`standard/data.csv.gz`: one row per school per survey year, 2012-13 to
2024-25, with county FIPS from the county name, school id, district and
school type, per-disease immunized shares (`rate_immunized_<disease>`),
`rate_medical_exemptions` and `rate_religious_exempt`. Rows are sorted
before writing because Socrata returns them in a different order on each
download.

No county aggregation is done here. County FIPS come from
`join_county_fips()`, which matches on a normalised name (so the 141 rows
the 2019-20 onward file spells "St.Lawrence" resolve to St. Lawrence) and
stops on any label it cannot account for. Three misspellings in the older
file are recoded before the join. 11 rows of the newer file carry no
usable county ("ERROR: #N/A" on 8, blank on 3) and are dropped; the ingest
logs both counts.
