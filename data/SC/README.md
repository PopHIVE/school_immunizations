# SC

South Carolina Department of Public Health (DPH), school immunization
certification by school and religious exemptions by county.

## Sources

- 45-day reports: "45-Day Report of Schools with Required Immunization
  Certification", one PDF per school year, linked from the DPH school
  vaccination coverage data page and discovered from it (see
  `sources.json`). 2024-25 and 2025-26 are posted, both dated 2026-03-10.
  Each is one school-level table: region, county, type (Public/Private),
  school name, city, grade range, total students, and the percent of
  students with the required immunizations. Kept as
  `raw/45_day_report_<start year>.pdf`. DPH reissues reports under a new
  dated file name, so a report is re-fetched when the posted name changes
  and the latest year is checked on every run.
- County exemptions: `raw/SC_2019-23.csv`, religious exemption counts and
  shares by county and statewide for 2018-19 to 2022-23, transcribed from
  the DPH five-year report. The county coverage and exemption page is
  Tableau Public with data access disabled, so nothing is fetched from it.

## Output

`standard/data.csv.gz` with a `type` column:

- `school` rows (1,516 for 2024-25, 1,520 for 2025-26) indexed by
  `school_name`, `school_type`, `city` and `grade_range`, with `N_enrolled`
  and `rate_utd`. The report prints a whole percent and top-codes it at
  ">96%" for about 30% of schools; those rows carry `rate_utd = 0.96` with
  `flag_utd = "top_coded"`. A few small schools print "<5%"
  (`bottom_coded`, 0.05), and "<10" students is `N_enrolled = 10` with
  `flag_enrolled = "bottom_coded"`. County FIPS comes from the county
  column; the 23 rows per year under "Lexington/Richland" (a district
  spanning both counties) have no FIPS. No county aggregate is computed from
  the school rows, because the top-coding makes the mean undefined.
- `county` and `state` rows (46 counties plus a statewide row, 2018-19 to
  2022-23) with `N_enrolled`, `N_personal_exempt` and
  `rate_personal_exempt` (religious exemptions).

## Parsing notes

The PDF table is set with single spaces between some columns and cuts long
cells off at the column edge (region "Low Countr", county "Spartanbur",
"ChesterfieldPrivate" where county and type touch, and twice a school name
run into its city). The ingest reads word coordinates from
`pdftools::pdf_data()`: the count and percent come from the end of each
line, region, county and type from the left by vocabulary, and the school
name, city and grade range are split at the column start positions found
for each file. Truncated county names are completed against the FIPS list;
a school name joined to its city is split at the longest tail that is a
city seen elsewhere in the report. The run stops on an unrecognised line,
an unknown school type, a report with fewer than 1,000 schools, or a school
count that moves more than 5% between years.
