# OK

Oklahoma State Department of Health, Kindergarten Immunization Survey.

## Sources

Both are linked from the OSDH "Immunization Survey / Shot Records" page
and discovered from it (see `sources.json`):

- County summary tables, one workbook per school year from 2021-22. The
  2019-20 and 2020-21 tables were taken down and survive only as the
  committed copies. OSDH renamed the files in 2024-25, so the ingest derives
  `raw/OK_County_<YY-YY>.xlsx` from whichever year spelling the name has.
- School-level results, one workbook covering every survey year since
  2017-18 with one sheet per year, replaced in place each summer. Stored as
  `raw/KSurvey_OSDH_SchoolLevelResults.xlsx` so the next edition overwrites
  it.

## Output

`standard/data.csv.gz` with a `type` column: `county` rows, a `state` row
per year, and `school` rows (with district, city and school type). Measures
are the share of kindergarteners up to date per antigen (`rate_utd_<vax>`),
up to date for all six (`rate_utd_all`), and the share with a medical,
non-medical or any exemption (`rate_medical`, `rate_non_medical`,
`rate_total`). School rows carry only the any-exemption share. 2017-18 to
2025-26.

"NR" (no response) and "*" (fewer than 10 kindergarteners) become NA. County
figures are taken as published; school rows carry no enrollment to
aggregate by.
