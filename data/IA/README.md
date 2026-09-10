# IA

Iowa HHS (formerly IDPH) Immunization Program, annual school immunization
audit.

## Sources

- Kindergarten Summary Report by County: one PDF per school year on
  publications.iowa.gov, found through the site's search page (see
  `sources.json`). Posted years are 2012-13, 2018-19, 2020-21, 2021-22,
  2023-24, 2024-25 and 2025-26; no report for 2013-14 to 2017-18, 2019-20
  or 2022-23 is on the site (checked 2026-09-08 with several queries and
  every results page). Search results come 20 to a page, so the ingest
  walks `search_offset` until a page has no matching link. Files are kept
  as `raw/kindergarten_county_<YYYY>-<YY>.pdf` and never re-requested once
  they open as a kindergarten summary. The 2012-13 file is titled
  "kindergarten 2012-2013" and has the same eight columns.
- K-12 medical and religious exemption certificates by county, 2011-12 to
  2024-25: one UTF-16 tab-delimited CSV per year and exemption type,
  exported by hand into `raw/K-12/Medical Exemption/` and
  `raw/K-12/Religious Exemption/`. `raw/Iowa Vaccine Exemption Data_K-12.xlsx`
  is the same data as a workbook and is not read.

## Output

`standard/data.csv.gz`, one row per county and school year, with `type`
(`county`, or `state` for the kindergarten statewide total, geography
`19`) and `grade`:

- `grade = "Kindergarten"` (99 counties plus the state row per year):
  `N_immunization_certificate`, `N_provisional`, `N_medical_exempt`,
  `N_religious_exempt`, `N_no_certificate` (invalid or no certificate),
  `N_valid_certificate` (the four certificate kinds together), `N_enrolled`,
  and each count over `N_enrolled` as `rate_immunization_certificate`,
  `rate_provisional`, `rate_medical_exempt`, `rate_religious_exempt`,
  `rate_no_certificate`, `rate_valid_certificate`.
- `grade = "K-12"` (99 counties per year): `N_medical_exempt`,
  `N_personal_exempt` (religious certificates, under the name the series has
  always used), `N_full_exempt`, `N_enrolled`, and the matching
  `rate_medical_exempt`, `rate_personal_exempt`, `rate_full_exempt`.

Every rate is computed from the counts. The kindergarten report's
"Percent Valid Certificates" column is only checked against
`N_valid_certificate / N_enrolled`: the reports round it twice (to two
decimals, then to one), so a few counties a year print 0.05 points above
the exact ratio, and 2025-26 prints a bare "100%" for four counties whose
counts give 99.5-99.6%. The check allows one unit of the last printed
digit and stops the run on anything further off. The parser also stops if
a report does not have exactly 100 data rows, if Total Valid is not the
sum of the four certificate columns, if Total Enrollment is not Total
Valid plus Invalid, or if the state row is not the sum of the counties.

## Caveats

- The two grades come from different files with different denominators
  (kindergarten enrollment against K-12 enrollment) and are not comparable
  to each other.
- In the K-12 series the medical and religious files are joined on the
  county's enrollment; `rate_full_exempt` is the combined count over that
  denominator rather than the sum of the two rates.
