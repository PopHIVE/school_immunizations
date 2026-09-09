# TN

Tennessee Department of Health, Kindergarten Immunization Compliance
Assessment (the annual kindergarten survey). Public schools are assessed
from Department of Education rosters; private schools respond to a survey
and about half to three quarters of them do in a given year.

## Sources

All four are listed in `sources.json`.

- `kindergarten_compliance_pdf`: the annual assessment report, one PDF per
  school year, 2019-20 to 2024-25, linked from
  https://www.tn.gov/health/immunization.html and kept as
  `raw/kindergarten_compliance_<start year>.pdf` (TDH names the files
  differently each year; `2020_KindergartenSurveyReport.pdf` is the 2019-20
  report). The ingest discovers new reports from that page and keeps
  whatever is already in `raw/`. Two parts of each report are read:
  - Appendix 1, Table 1: per county the percent of students fully
    immunized, the number of students, and the number of schools with
    95-100%, 90-94.9% and under 90% of students fully immunized. Public and
    private schools are combined from 2020-21 on; the 2019-20 report has
    public schools in Table 1 and private schools in Table 2. The three
    band columns changed order in 2023-24, so the ingest reads the order
    from the header cell positions.
  - The public and private school statewide summaries: students fully
    immunized (n of N) and the count and share in each not-fully-immunized
    category (religious exemption, incomplete record, missing record,
    temporary certificate, transfer within 30 days, medical exemption).
  The county pages of Appendix 2 are images and are not read.
- `kindergarten_school_reports_pdf`: for 2017-18 and 2018-19 the assessment
  was published as school listings instead, one PDF each for public and
  private schools, linked from the same page and kept as
  `raw/kindergarten_schools_<start year>_<public|private>.pdf` (TDH's
  `2018_Public_Report_FINAL.PDF` is 2017-18, named by the school-year end;
  `2018_19_Public_Report.pdf` is 2018-19). Each lists every school with its
  enrollment and the count and share in each category, with a COUNTY TOTAL
  row per county and a STATEWIDE TOTAL row at the end. Only the total rows
  are read. The county name is printed once per block of rows, vertically
  centred and usually on a line of its own, so the total rows are matched
  to it by word position rather than by text line. The private listings
  have no transfer column.
- `kindergarten_survey_csv`: `KINDERGARTEN_SURVEY.csv` from the TDH
  coverage-rate dashboard, one row per responding school with enrollment
  and the count in each category, school years 2019-20 to 2021-22 (last
  modified August 2023). Kept as `raw/KINDERGARTEN_SURVEY.csv` and
  refreshed when the server reports a change.
- `tdh_mmr_county`: the 2024-25 county kindergarten MMR workbook TDH
  supplied to PopHIVE (`raw/KMMRCoverage_County.xlsx`), fetched only if the
  committed copy is missing.

### Page subsets in raw/

The compliance reports are 8-29 MB each as posted (80 MB for six years),
nearly all of it the Appendix 2 county images. The ingest downloads a
report to a temporary file, parses it, and writes only the pages the parser
read (the two statewide summary sections and the Appendix 1 tables, 8 to 11
pages a year) to `raw/` with `qpdf::pdf_subset()`; the subset is parsed
again and must give the same result as the full file. `raw/` therefore
holds subsets of under 1 MB each, and `fetch_state` in `process.json`
records the subset's size and hash, not the posted file's. A report whose
subset is in `raw/` and parses is not downloaded again; delete the subset
to re-fetch it. The school listings are read page by page, so their
subsets are every table page (only the cover page with the category
definitions is dropped).

## Outputs

Grade is Kindergarten throughout; `time` is the school-year start.

- `standard/data.csv.gz`: county rows (`type = "county"`) and statewide
  rows (`type = "state"`, geography 47).
  - From the report tables, 2019-20 to 2024-25: 95 county rows per year
    (93 in 2023-24 and 2024-25, when Lauderdale and Pickett did not
    submit), with `school_type` "all" (2020-21 on) or "public" and
    "private" (2019-20, 95 and 30 rows). Measures are
    `rate_fully_immunized`, `N_enrolled`, `N_schools_95plus`,
    `N_schools_90_94`, `N_schools_under_90` and their sum `N_schools`. The
    2024-25 combined rows also carry `rate_mmr` from the MMR workbook.
  - From the school listings, 2017-18 and 2018-19: public (95 counties)
    and private (25 and 24 counties) rows with `N_enrolled`,
    `N_fully_immunized`, `rate_fully_immunized`, and the count and rate in
    each category (`N_`/`rate_` `religious_exempt`, `medical_exempt`,
    `temporary_certificate`, `incomplete`, `missing`, and `transfer` on
    public rows only). These years have no school counts and no combined
    public-and-private county rows.
  - Statewide rows for each year 2017-18 to 2024-25 and school type carry
    `N_enrolled`, `N_fully_immunized`, and the count and rate for each
    category. The "all" state row is the sum of the public and private
    rows. Statewide rates are computed from the counts; the printed shares
    are checked against them.
- `standard/data_survey_counties.csv.gz`: county sums of the survey file by
  school type (public, private, all), 2019-20 to 2021-22, with
  `N_schools`, `N_enrolled` and the category counts and rates.
- `standard/data_schools.csv.gz`: the survey file's school rows
  (`type = "school"`, `school_name`, 3,545 rows) with the same measures.
  The 2017-18 and 2018-19 listings' school rows are not written.

## Caveats

- The survey file and the report tables agree on statewide totals but not
  on county assignment in 2019-20: 38 of 95 counties have different
  enrollment (Anderson 763 students in the file, 853 in the report) and
  the school counts differ too, so the file evidently places some schools
  in a different county from the report. Carter County also differs in
  2020-21 (541 against 550) and 2021-22 (one school). The two sources are
  therefore written to separate files and neither fills the other.
- Private schools have no transfer figure ("N/A" in the summaries, no
  column in the listings), so `N_transfer` and `rate_transfer` are blank on
  private rows and on combined state rows.
- The 2022-23 private school summary prints "Missing Record: 1.1% (n=107)";
  107 is the 2021-22 count and 1.1% of 5,097 is 56. The count is dropped
  for that cell and the printed share kept, and the combined 2022-23
  missing-record count is blank as a result. 2023-24 prints medical
  exemptions as 0.0% with n=94 (0.13%); the rate from the count is kept.
- The 2018-19 private listing carries Sevier County as two blocks
  ("Sevier", one school, and "Sevier County", one school); they are summed
  into one county row.
- The public statewide fully immunized count is 71,949 in both 2017-18 (of
  75,480) and 2018-19 (of 75,880). In both years it is the sum of the
  county rows and the other categories differ, so it is a coincidence, not
  a copied figure.
- The 2017-18 private listing excludes schools with fewer than 10
  kindergarten students; the 2018-19 listing includes them.
- In the survey file 143 school rows have category counts that do not sum
  to enrollment, and one (Keenburg Elementary, Carter County, 2020-21)
  reports 48 fully immunized of 24 enrolled. Values are written as
  published.
- Report county rows for 2019-20 on have no category counts: those tables
  are images.
- tn.gov intermittently resets connections and answers 404 for pages that
  exist; the ingest treats an unreachable index page or file as a warning
  and parses the committed copies. The former program page
  (`health/cedep/immunization-program.html`) is gone.
