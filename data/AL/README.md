# AL

Alabama Department of Public Health, Immunization Division, School Entry
Survey.

## Sources

- County summary PDFs on the survey page
  (https://www.alabamapublichealth.gov/immunization/school-entry-survey.html),
  found by `discover_links()` with the pattern in `sources.json` and kept in
  `raw/` under their posted names. They never change once posted, so a copy
  that validates as a PDF is not re-requested. If the page cannot be
  fetched, the URLs recorded in `process.json` from earlier runs are used
  to confirm the on-disk files, and the parse proceeds on `raw/` either way.

  | File | School year | Table |
  |---|---|---|
  | `2014-2015schoolentrysurvey_web1.pdf` | 2014-15 | All grades, public and private |
  | `2015-2016schoolsurvey_web.pdf` | 2015-16 | All grades, public and private |
  | `2016_2017_schoolsurvey.pdf` | 2016-17 | All grades, public and private |
  | `2017-2018schoolsurvey.pdf` | 2017-18 | All grades, public and private |
  | `2018-2019schoolsurvey.pdf` | 2018-19 | All grades, public and private |
  | `2019-2020schoolsurvey_county.pdf` | 2019-20 | All grades, public schools only |
  | `2020schoolsurvey_county.pdf` | 2020-21 | All grades, public and private |
  | `2020schoolsurvey_kindergartenandcounty.pdf` | 2020-21 | Kindergarten, public and private |

  Each is one column-aligned table: a row per county grouped under a
  public-health district (with a district `TOTAL` row, dropped) and a
  statewide row. The columns are counts with a percent of enrolment beside
  each: enrolled; holding a current ("not expired") Certificate of
  Immunization; Certificate of Medical Exemption; medical exemption together
  with a certificate (a partial exemption, up to date otherwise); the same
  two for religious exemptions; expired certificate; no certificate on file.
  The order of these columns changes between years and the 2020-21 tables
  omit the no-certificate column, so `ingest.R` declares a layout per file
  (`PDF_LAYOUTS`) and stops on a file it has no layout for. 2014-15 and
  2015-16 split the medical exemption into permanent and temporary (summed
  here) and carry a Td/Tdap column that is not used.

  The page also links two school-level files
  (`2019-2020schoolsurvey_school.pdf`, `2020schoolsurvey_schoolandcounty.pdf`)
  and the survey packet and instructions; the pattern leaves them out and
  they are not parsed. Nothing has been posted since the 2020-21 tables.

- Exemption workbooks by grade obtained from ADPH by request, in `raw/`:
  `Kindergarten school exemptions 2021-2025 (2).xlsx`,
  `Seventh grade school exemptions 2021-2025 (2).xlsx`,
  `Ninth grade school exemptions 2021-2025.xlsx`. One sheet per report run,
  named for the run date; the i-th sheet of each workbook is the school year
  ending in the i-th year of the range in the file name, 2020-21 to 2024-25.
  Counts and proportions of students with a full medical or religious
  exemption and with a partial exemption while up to date. The workbook
  does not say whether private schools are included.

## Output

`standard/data.csv.gz`, one row per county (`type = "county"`) or statewide
total (`type = "state"`, geography `01`) per school year, grade and source:

- `source`: `survey_pdf` or `request_workbook`. 2020-21 kindergarten is
  present from both; the figures differ slightly (56,974 enrolled in the
  PDF, 56,201 in the workbook) because the workbook report was run on a
  different date, and both are kept.
- `grade`: `All grades` and `Kindergarten` for the PDFs; `Kindergarten`,
  `7th grade`, `9th grade` for the workbooks.
- `school_type`: the population a PDF table covers, `public and private`
  or `public` (2019-20). The two 2020-21 tables also print a statewide
  split, kept as state rows with `school_type` `public` and `private`. NA
  for the workbook rows.
- Measures: `N_enrolled`; `N_valid_certificate` / `rate_valid_certificate`
  (current certificate of immunization, the coverage measure);
  `N_full_medical_exempt`, `N_partial_medical_exempt_utd`,
  `N_full_religious_exempt`, `N_partial_religious_exempt_utd` and their
  rates; `N_medical_exempt` and `N_religious_exempt` (the full exemptions
  under the canonical names) and `N_full_exempt` (their sum) with rates;
  `N_expired_certificate` / `rate_expired_certificate`;
  `N_no_certificate` / `rate_no_certificate` (PDFs to 2019-20 only). The
  workbook rows carry only the enrolment and exemption columns.
- Rates for the PDF rows are each count over `N_enrolled`; the printed
  percentages are checked against that ratio to 0.1 of a point and not
  used. The workbook rows keep the published proportions, which equal
  count / `N_enrolled`. `flag_<measure>` records what the PDF printed in
  place of a percent: `missing` for Bullock and Macon in 2017-18, which
  reported no students (`#DIV/0!` in the source, counts 0, rates NA).

Every PDF table is checked to yield 67 counties, and its statewide row
(and the public/private split) to equal the county sum.

## Caveats

- `2020schoolsurvey_county.pdf` is titled "2019-2020 School Entry Survey"
  but is the 2020-21 survey: the page lists it as "2020 by County" (the
  page names each file by the school-year start, as "2019 by County" for
  the 2019-20 file), its totals are identical to the 2020-2021 school-level
  file posted beside it, and its 836,374 enrolled is a different population
  from the 716,301 of the 2019-20 public-schools table. It is dated
  2020-09-01. The 2019-20 table's footnote says "public and private" while
  its title says "Public Schools"; the title is followed.
- The 2020-21 all-grades table prints Baldwin on two lines (40,353 and a
  second "Baldwin" line of 132 students that the district total includes);
  the counts are summed.
- Some printed figures look like source errors and are carried as printed:
  Shelby and St. Clair in 2016-17 appear to have each other's rows
  (St. Clair 36,048 enrolled, Shelby 13,523); Lowndes in 2017-18 repeats
  Lee's exemption counts (68 religious, 19 partial medical, 60 partial
  religious against 1,700 enrolled); Greene in 2015-16 reports 7 students
  and in 2016-17 none. Several district `TOTAL` rows carry a wrong percent
  (2017-18 and 2018-19 SERN medical), which does not matter since district
  rows are dropped.
- Alabama has no personal or philosophical exemption, so no
  `N_personal_exempt` is emitted.
