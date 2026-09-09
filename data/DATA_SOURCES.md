# School Immunization Data Sources by State

This document records, for every state, where the school immunization data
comes from, how it is accessed (open-data API, static file, dashboard, or
records request), what the ingest currently produces, and which public
sources exist that are not ingested yet. The machine-readable version of the
source list is `data/<ST>/sources.json`; this file is the narrative.

Compiled 2026-07-24. Updated 2026-08-25 (DE, GA, NE, WY notes). Rewritten
2026-09-08 after a source survey against the Washington Post school
vaccination inventory and a rework of the download layer; see "What changed
in September 2026" at the end.

## How sources are tracked

Every state folder carries a `sources.json`: one entry per source with an
`id`, an `access` type (`socrata`, `arcgis`, `ckan`, `static`, `index_page`,
`report_viewer`, `dashboard`, `manual`, `request`), the `url` or `page_url`
and discovery `pattern`, the month the agency usually publishes
(`publish_month`, `publish_lag_years`), the latest school year the ingest
has produced (`latest_year_ingested`, written by the ingest), and whether
the URL can be reached from GitHub Actions (`ci_reachable`).

Network ingests use `resources/fetch.R`, which downloads to a temporary
file, validates the content (workbook signature, CSV header, PDF magic,
bot-block page detection), and only then replaces the committed copy. A
failed transfer keeps the committed file, emits a warning, and records
`status = "failed"` in the `fetch_state` block of `process.json`. Conditional
requests (ETag, Last-Modified) avoid re-downloading unchanged files, and the
parse step only runs when a raw file or the script changed.

Two scripts report on the state of things:

- `scripts/check_sources.R` (weekly, `.github/workflows/check_sources.yaml`)
  checks every declared URL, runs each discovery pattern, lists files on an
  index page that are not in `raw/` yet, and flags states whose latest
  ingested year is behind what the publishing calendar says should exist.
  It writes `data/SOURCE_STATUS.md` and never runs a parser.
- `scripts/build_status.R` (nightly, at the end of `build.yaml`) reads every
  `process.json` and writes `data/BUILD_STATUS.md`; the job fails if an
  ingest errored or a fetch failed with no committed copy to fall back on.

## What each state produces

Read from `standard/*.csv.gz` on 2026-09-08. "Coverage" means at least one
per-antigen or overall up-to-date rate; "MMR" means an MMR-specific rate is
present; "Exempt" means at least one exemption rate. Row types are the values
of the `type` column (or school when the file is school-level without one).

| State | School years | Row types | Coverage | MMR | Exempt | Access |
|---|---|---|---|---|---|---|
| AK | 2023-24 to 2024-25 | region, county, state | Y | Y | - | manual |
| AL | 2014-15 to 2024-25 | county, state | Y (valid certificate) | - | Y | index_page (PDF), request |
| AR | 2013-14 to 2024-25 | district | - | - | Y | request |
| AZ | 2010-11 to 2023-24 | county | Y | Y | Y | dashboard |
| CA | 2018-19 to 2024-25 | county | Y | Y | - | static, manual |
| CO | 2017-18 to 2025-26 | county, state, district, school | Y | Y | Y | arcgis |
| CT | 2012-13 to 2025-26 | county (planning region), school | Y | Y | Y | socrata, manual |
| DC | none | - | - | - | - | request |
| DE | 2016-17 to 2022-23 | state | Y | Y | Y | index_page (chart PDFs) |
| FL | 2006-07 to 2025-26 | county | Y | - | Y | report_viewer, manual |
| GA | none | - | - | - | - | none |
| HI | 2014-15 to 2024-25 | county, school | - | - | Y | index_page |
| IA | 2011-12 to 2025-26 | county, state | Y (certificate status, K) | - | Y | index_page (PDF), manual |
| ID | 2018-19 to 2024-25 | county | - | MMR exempt only | Y | request |
| IL | 2019-20 to 2024-25 | county | Y | - | Y | manual |
| IN | 2023-24 to 2025-26 | county | Y | Y | - | ckan |
| KS | 2019-20 to 2023-24 | county | - | - | Y | manual |
| KY | 2019-20 to 2024-25 | county | - | - | Y | static, manual |
| LA | 2021-22 to 2024-25 | parish | Y | Y | Y | manual |
| MA | 2013-14 to 2025-26 | county | Y | Y | Y | index_page (not CI reachable) |
| MD | 2019-20 to 2025-26 | county, school | Y | Y | Y | index_page |
| ME | 2018-19 to 2024-25 | school | Y | Y | Y | index_page |
| MI | 2018-19 to 2024-25 | county, school | Y (overall) | - | Y | index_page |
| MN | 2023-24 to 2025-26 | county | Y | Y | - | index_page |
| MO | 2019-20 to 2024-25 | county | - | MMR exempt only | Y | dashboard |
| MS | 2023-24 | county | - | - | counts only | static, manual |
| MT | 2016-17 to 2020-21 | county | Y | Y | Y | manual (collection ended) |
| NC | 2020-21 to 2023-24 | county | - | - | Y | dashboard |
| ND | 2018-19 to 2024-25 | county, school | Y | Y | Y | dashboard |
| NE | none | - | - | - | - | none |
| NH | 2021-22 to 2024-25 | county | - | - | Y | manual |
| NJ | 2013-14 to 2024-25 | county | - | - | Y (medical only) | manual |
| NM | 2011-12 to 2023-24 | county | - | - | Y | manual |
| NV | 2009-10 to 2024-25 | county | Y | Y | Y | request |
| NY | 2012-13 to 2024-25 | school | Y | - | Y | socrata |
| OH | 2024-25 | county | - | MMR exempt only | Y | dashboard |
| OK | 2017-18 to 2025-26 | county, school, state | Y | Y | Y | index_page |
| OR | 2024-25 | county, school | Y | Y | Y | static |
| PA | 2020-21 to 2025-26 | county | Y | Y | Y | index_page |
| RI | 2018-19 to 2024-25 | state | - | MMR exempt only | Y | request |
| SC | 2018-19 to 2025-26 | county, state, school | Y (school rows, top-coded) | - | Y | index_page (PDF), dashboard |
| SD | 2017-18 to 2023-24 | county, school | - | - | counts only | request |
| TN | 2017-18 to 2024-25 | county, state, school | Y | Y (2024-25) | Y | index_page (PDF, csv), static |
| TX | 2013-14 to 2025-26 | county, district, state | Y | Y | Y | index_page |
| UT | 2018-19 to 2022-23 | county | - | - | Y | request |
| VA | 2019-20 to 2024-25 | county, school | - | - | Y | manual |
| VT | 2017-18 to 2024-25 | county | Y | Y | Y | request |
| WA | 2014-15 to 2025-26 | county, district, state, school (2014-15 to 2016-17) | Y | measles, mumps, rubella separately (MMR in the school rows) | Y | index_page, socrata |
| WI | 2018-19 to 2024-25 | county, school | Y (registry MMR) | Y | Y | static, arcgis, manual |
| WV | 2017-18 to 2025-26 | county | - | - | counts only | request |
| WY | 2018 to 2023 (registry), 2022-23 to 2023-24 (K) | county, state | Y (registry) | Y (registry) | counts only | manual (report-card workbooks) |

Gaps that remain after this pass:

- No output: DC, GA, NE.
- Exemptions only, no coverage: AR, HI, ID, KS, KY, MO, NC, NH, NJ, NM, OH,
  RI, UT, VA. The public sources that exist for these are PDFs whose figures
  are charts, dashboards, or pages with no file link (see "PDF and dashboard
  sources that could not be parsed" below).
- Coverage only, no exemptions: AK, CA, IN, MN.
- Single year: MS, OH, OR.
- No county rows: AR (district), DE (state), ME and NY (school only), RI
  (state).
- Counts only, no rates: MS, SD, WV; WY exemptions are waiver counts.

## Sources by access type

### Open-data endpoints

| State | Source | Notes |
|---|---|---|
| CO | CDPHE ArcGIS `OPEN_DATA/cdphe_sccidr` MapServer, tables 1 (county), 4 (statewide), 2 (district), 5 and 6 (facility, 2017-18 to 2022-23 and 2023-24 on) | Paginated REST queries. The two facility tables hold 1.6 million rows, so they are fetched with a metric filter (fully immunized, medical exemption, non-medical exemption) and stored compressed. The facility ID is stable within a year only. |
| CT | data.ct.gov Socrata `8kid-pp5k` (county and planning region, Pre-K/K/7th) | Plus school-level `iux5-vrzq` (K), `rz57-x4bb` (7th), `a2a4-pw6c` (all-grades exemptions). The three school datasets keep one id across years and are overwritten in place each year, so only the current year can be fetched; earlier years are kept from committed snapshots. The all-grades exemption workbook in `raw/` is still the only multi-year all-grades county figure. |
| IN | IDOH open-data hub (CKAN), package `immunization-division-s-school-supplemental-dashboard` | Per-year workbooks enumerated through `package_show`. No medical/religious split in the hub file. |
| NY | health.data.ny.gov Socrata `btkd-y8bp` (2019-20 on) and `5pme-xbs5` (2012-13 to 2018-19) | School-level. County FIPS from the county name; 152 rows of the 2019+ file with unusable county labels are dropped. |
| WA | data.wa.gov Socrata school-level datasets 2014-15 to 2016-17 (`3nrj-de9w`, `i89p-imif`, `raxi-vijr`, `9vf7-7een`, `ie96-cgrn`, `kck7-yb2v`, `emhz-m99x`, `9zru-c2kz`) | Closed series, fetched once. The dataset titled sixth grade 2014-15 is a copy of that year's K-12 table and is skipped. School sums match the DOH 2016-17 statewide rows within 0.1 percent. |
| WI | DHS ArcGIS `DHS_IMMZ/School_Immunization_Rates` MapServer (layer 0 schools, layer 1 districts) | Current-year waiver percentages as numbers rather than the "<5%" text of the PDFs. |

### Static files on agency pages

| State | Source | Notes |
|---|---|---|
| CA | CDPH kindergarten and 7th-grade report workbooks (county tables); CHHS school-level open data for older years | Two public CDPH workbooks listed in `sources.json`; no public source for 2023-24. |
| HI | DOH immunization/examination report page | PDF through 2023-24 (transcribed by hand into `raw/`), xlsx from 2024-25, discovered from the page. No 2020-21 report exists. |
| MA | mass.gov current and archive pages, per-year by-county K and 7th-grade workbooks | The mass.gov WAF rejects datacenter IPs, so `ci_reachable = false`: CI processes the committed files, and new years must be fetched from a normal connection. |
| MD | MDH "Percent of Kindergarteners Vaccinated by School" workbooks, 2019-20 to 2025-26 | School rows kept, county rows enrollment-weighted from them. 2024-25 and 2025-26 were added in this pass. The separate by-county page publishes PDFs only (checked 2026-09-08). |
| ME | Maine CDC data-reports page, per-year "School Vaccination Rates" workbooks | K/7th/12th, school rows. |
| MI | MDHHS school immunization data page, building-level workbooks for K, 7th and new entrants | The page blocks some bots; discovery falls back to the committed files. Year in the file name is the school-year end. |
| MN | MDH `kcounty<yy><yy>.xlsx` per year | County K coverage and exemptions by antigen. |
| OK | OSDH county summary workbooks per year and the school-level results workbook (2017-18 to 2025-26) | School rows carry the county; the statewide row has `type = "state"`. |
| OR | OHA `SchK-12.xlsx`, overwritten each fall at a fixed URL | Current year only; the previous year's file is not kept online. |
| PA | pa.gov immunization rates page, per-year "Survey Summary by County" workbooks (2020-21 to 2025-26) | Links are embedded in a JSON attribute rather than plain anchors; `discover_links()` handles both. |
| TN | TDH kindergarten MMR county workbook supplied to PopHIVE; annual compliance assessment PDFs 2019-20 to 2024-25, the 2017-18 and 2018-19 public and private school listings (county totals), and the KINDERGARTEN_SURVEY.csv export, all discovered from tn.gov/health/immunization.html | The CEDEP program page recorded earlier is gone (404). `raw/` holds page subsets of the PDFs (the pages the parser reads, cut with qpdf), 4 MB in place of 80 MB; the full reports are downloaded to a temp path and discarded. |
| TX | DSHS coverage page: per-year K and 7th-grade "coverage by district and county" workbooks (2019-20 to 2025-26) and the multi-year conscientious exemption workbooks | Replaces the hand-built exemption CSVs, which had eleven counties' values misaligned. District rows in `data_districts.csv.gz`. |
| WA | DOH school immunization data tables, one workbook per school year (2016-17 to 2025-26) | County, district and state rows; per-antigen coverage, exemption and incomplete counts. The earlier records-request workbook is no longer parsed. |
| WI | DHS `mmr-map-data-2019-2024.xlsx` (registry MMR by county and age group) and per-year school waiver workbooks | Registry rows describe resident children, not a school cohort; they carry age-specific names (`rate_mmr_2dose_6y`, etc.). |
| AK | Quarterly VacTrAK coverage report workbooks | Supplied to the project, not downloaded; no fixed URL for the current quarter. |
| IA, KY, KS, MS, NH, NJ, NM, UT, VA, IL, LA | Committed workbooks or transcriptions of agency PDFs | See per-state notes in `sources.json`. |

### Dashboards without a file endpoint

| State | Source | Notes |
|---|---|---|
| AZ | ADHS IDRReportStats query tool and per-year county MMR PDFs | Raw CSVs were captured by hand; 2024-25 PDF exists and is not parsed. |
| FL | FLHealthCHARTS kindergarten report viewer (ASP.NET) | Automated through the report viewer's export; exemptions from a committed scrape. |
| MO | Tableau (launched December 2024) | No export found. |
| NC | JavaScript kindergarten dashboard | No export found. |
| ND | Power BI | No export found. |
| NJ | Status-report dashboard; historical PDFs by year, grade and exemption type | PDFs are parseable; see below. |
| OH | DataOhio "Annual Ohio Kindergarten Immunization Level Assessment", 2017-18 to 2025-26, county and school | State Tableau Server with guest access and no data endpoint; the single ingested year is a hand export. |
| SC | DPH county coverage and exemption page; Tableau Public with data access disabled | The 2024-25 45-day report is published as a PDF. |
| SD, UT, VT, WA (dashboard), WI (web app) | Dashboards alongside the files listed above | Not needed where a file exists. |

### Records request only

| State | What the agency has provided | Notes |
|---|---|---|
| AL | County kindergarten table to the Washington Post | ADPH survey page publishes state totals only. |
| AR | Act 676 by-district exemption workbook | No county file exists. |
| DC | County (District-wide) and school MMR rates to the Washington Post | dchealth.dc.gov answers HTTP 403 to every client tried from outside a residential connection, so the public "MMR Rates by School" page cannot be fetched from CI. `data/DC/` is a stub with the source recorded. |
| GA, NE | County kindergarten rates to the Washington Post | No public file or dashboard found. |
| ID | Exemption workbook supplied to the project | Dashboard publishes coverage that is not ingested. |
| MT | Collection ended after 2018-19 by law; 2024-25 school-level data released to press on request | |
| NV | County series supplied; likely CDC SchoolVaxView derived | |
| RI | Religious exemption counts | ArcGIS hub page returned no dataset links. |
| SD, WV, WY | Workbooks supplied to the project | WY files are in `raw/` and not parsed. |

## Public sources found and not yet ingested

These were confirmed during the September 2026 survey. Each needs a parser,
not a new download mechanism. `resources/pdf_table.R` (added 2026-09-08)
reads column-aligned tables and chart labels out of text PDFs; the states
whose reports are real tables were ingested with it (IA, TN, AL, SC, DE, and
WY from its xlsx cards), and the rest are listed here with the reason.

### PDF and dashboard sources that could not be parsed

| State | Source | Why not |
|---|---|---|
| MS | MSDH "School Immunization Compliance Report" 2021-22 to 2024-25 and "Medical and Religious Exemptions Report" 2022-23 to 2024-25 (linked from msdh.ms.gov page 14,0,71,63) | County figures are drawn as bar charts and choropleth maps with no value labels in the text layer; only region and state totals are text. |
| KY | "Annual School Immunization Coverage Assessment Report for Kentucky Counties" | The 2024-25 PDF URL answers 404 and the healthtracking.ky.gov pages render their links in JavaScript; no file link found. |
| NJ | Status-report PDFs by year, grade and exemption type | The dashboard page links only a methodology PDF; the per-year document directories are not listable. |
| UT | immunize.utah.gov school immunization data (kindergarten and 7th grade) | Now an Adobe Captivate HTML presentation, plus 160-page coverage reports whose school section is per local health district; no county table. |
| KS | KDHE kindergarten immunization data and coverage pages | Pages carry no file links; the county report cards are behind the ArchiveCenter application. |
| VA | VDH SIS reports page and exemption dashboard | Only a step-by-step guide PDF and an embedded dashboard; no file. |
| AZ | ADHS 2024-25 county kindergarten MMR PDF | The table is the share of schools at or above 95% MMR coverage per county, not a coverage rate; the IDRReportStats app remains the source. |
| MI | 2024 seventh-grade building file | 404 on the media path. |

### Sources ingested from PDFs in this pass

| State | Source | What it adds |
|---|---|---|
| IA | publications.iowa.gov "Kindergarten Summary Report by County", 2018-19 to 2025-26 (found through the site search page) | County kindergarten certificate status: immunization certificate, provisional, medical and religious exemption, no certificate, enrollment |
| TN | TDH "Kindergarten Immunization Compliance Assessment Report" 2019-20 to 2024-25, the 2017-18 and 2018-19 school listings, and the dashboard export KINDERGARTEN_SURVEY.csv (school-level category counts 2019-20 to 2021-22), all linked from tn.gov/health/immunization.html | County share fully immunized, enrollment and school coverage bands for six years, county status categories for two more; statewide public, private and combined status categories; school rows and a second county file from the survey export |
| AL | ADPH school entry survey county summaries 2014-15 to 2020-21 | Six earlier years and a valid-certificate coverage measure alongside the request workbooks |
| SC | DPH 45-Day Report of Schools with Required Immunization Certification, 2024-25 and 2025-26 | School rows with the share of students with required immunizations (top-coded at 96%) |
| DE | DPH kindergarten coverage and immunization status charts, 2016-17 to 2022-23 | Statewide per-antigen coverage and exemption status (first DE data) |
| WY | County immunization report cards (registry-based), 2018 to 2023, from the 137 workbooks already in `raw/` | County registry coverage by age group and waiver counts (first WY data); the 2022 and 2023 cards add a kindergarten block from the school survey, written to `data_kindergarten.csv.gz` |

### Machine-readable sources not yet wired in

| State | Source | Would add |
|---|---|---|
| OH | DataOhio dashboard, if a data endpoint appears | County and school rows 2017-18 on |
| MI | Seventh-grade 2024 building file (404 on the media path) | Missing 7th-grade year for 2023-24 |

## Upstream sources that stay out of this repository

- CDC SchoolVaxView (`data.cdc.gov/resource/ijqb-a7ye`) carries state-level
  MMR, DTaP, polio, hepatitis B and varicella coverage plus medical,
  non-medical and any exemption rates for every state, 2009-10 to 2025-26.
  It is ingested in PopHIVE/Ingest as `schoolvaxview` and is the state-level
  fallback for every gap above.
- The Washington Post school vaccination files (county kindergarten MMR or
  overall rates for 44 states plus DC; school-level rates with exemption
  columns for 34 states plus DC) are ingested in PopHIVE/Ingest as
  `schoolvax_washpost`. Their `data_sourcing.csv` names the public URL or the
  records request behind every state and was the basis for the survey.

## Cross-check against the Washington Post county file

County kindergarten MMR (or nearest equivalent) from the new and refactored
ingests was compared with `vaxrates_counties.csv` on 2026-09-08 for the
years both carry (2018-19, 2019-20, 2023-24, 2024-25):

| State | Our measure | County-years | Result |
|---|---|---|---|
| TX | rate_mmr, kindergarten | 504 | identical |
| OK | rate_utd_mmr, kindergarten | 154 | identical |
| MD | rate_mmr, county rows | 24 | identical |
| CT | rate_mmr, K | 9 | identical |
| MN | rate_mmr | 87 | identical |
| WA | rate_measles, kindergarten | 39 | 95% within 2 points, largest gap 1.2 points |
| WI | rate_mmr_2dose_6y (registry) | 144 | median gap 1 point, largest 9; WaPo used an earlier vintage of the same registry file, which DHS restated in 2025 |

`scripts/compare_washpost.Rmd` does the same comparison for every state.

## Per-state caveats

- CO: district rows take a county FIPS only when every facility of the
  district sits in one county; 13 multi-county districts and five online
  schools have `geography` NA.
- CT: the county file switches from counties to planning regions around
  2022-23; school rows resolve to planning-region FIPS.
- HI: 2020-21 does not exist on the DOH page. `school_type` is normalised
  to Public, Private, Charter, DHS or Day Care Center. From 2024-25 the not-up-to-date
  total equals religious plus medical plus no record plus missing
  immunizations.
- FL: the report viewer re-exports the two kindergarten workbooks on every
  run with different bytes; the fetch compares cell contents
  (`content_key = workbook_content_key`) so an identical export leaves
  `raw/` and the parse gate alone.
- MA: `ci_reachable = false`; fetch new years from a residential connection.
- MD: MDH revises posted workbooks in place (the live 2023-24 file differs
  in layout and row count from the committed one). The ingest keeps the
  committed copy (`if_exists = "skip"`), so a revision is only picked up by
  deleting the raw file. Two copies of 2025-26 exist in `raw/`; the plainly
  named one is parsed.
- MI: the year in a file name is the school-year end. The "PROVISIONAL All
  Grades 2024" workbook is kept in `raw/` but not parsed, because final
  2024 kindergarten and new-entrants workbooks exist; the 2024 seventh-grade
  file returns 404, so 2023-24 has no seventh-grade rows.
- NY: county FIPS come from `join_county_fips()`, which resolves the
  "St.Lawrence" spelling; 11 rows of the 2019+ file with no usable county
  ("ERROR: #N/A", blank) are dropped and logged. Socrata returns rows in a
  different order on each download, so the ingest sorts before writing.
- OK: the statewide row has `type = "state"`; TX and WA follow the same
  convention.
- OR: the fixed-URL workbook holds one year; the archive is what is in
  `raw/`.
- TX: district sheets omit schools with five or fewer students in the grade;
  "NR" cells are flagged `missing`.
- WI: the registry MMR rows are by calendar year and resident age group, not
  by school cohort.
- TN: the survey CSV and the report tables disagree on school-to-county
  assignment for 38 counties in 2019-20, so they are written to separate
  files. Two counties did not submit in 2023-24 and 2024-25. `raw/` holds
  page subsets of the report PDFs; a subset that no longer parses is
  re-downloaded and re-cut.
- WY: the report cards are PDF-to-Excel conversions and 16 hand-typed
  workbooks; a few cells are lost or mistyped (2020 cards lack four measures
  for 19 counties, Converse 2018 is an image-only PDF and is missing). The
  "age 6" block is labelled 7-year-olds on the 2019 to 2021 cards. Card year
  N is dated N-09-01, one year later than the WI registry convention.
- AL: `2020schoolsurvey_county.pdf` is titled 2019-2020 but its totals match
  the 2020-21 files, so it is dated 2020-21. Column layouts differ by year
  and each file has a declared layout; a new file stops the run until one is
  added.
- DE: statewide only; the 2016-17 and 2017-18 coverage clusters label only
  the polio bar, so the other four antigens are NA for those years.
- IA: the published percent is rounded twice, so it can differ from the
  count ratio by one unit of the last digit.
- SC: the 45-day report is top-coded at ">96%" (written as 0.96 with
  `flag_utd = "top_coded"`), so county means cannot be computed from it.
- WY: health.wyo.gov sits behind Cloudflare bot management and answers 403
  to some requests minutes apart from the same client. The source check
  reports a 403 as unverifiable rather than as a dead page.

## What changed in September 2026

- Added `resources/fetch.R`, `data/<ST>/sources.json` for every state,
  `scripts/check_sources.R`, `scripts/build_status.R`, the weekly
  `check_sources.yaml` workflow, and a status step in `build.yaml`.
- Moved every network ingest onto the fetch layer: CA, CO, CT, FL, HI, IN,
  MA, MD, ME, MI, MN, NY, OK, OR, PA, TN, TX, WA, WI. CO, CT and NY no longer
  write downloads straight onto the committed raw file.
- New or extended data: TX coverage by antigen for K and 7th grade, 2019-20
  to 2025-26, county and district; WA per-antigen coverage, exemption and
  incomplete series 2016-17 to 2025-26; WI registry MMR county coverage and
  ArcGIS school waiver rates; OK school-level rows and 2017-18 to 2025-26;
  NY 2012-13 to 2018-19; CT school-level rows; MD 2024-25 and 2025-26; MI
  2023-24 and 2024-25 building files; HI 2024-25.
- Added `data/DC/` as a documented stub.
- Added `resources/pdf_table.R` (pdftools) and ingested the PDF-only
  sources that are real tables or labelled charts: IA kindergarten county
  audits 2012-13 to 2025-26, TN compliance reports 2019-20 to 2024-25, AL
  school entry survey county summaries 2014-15 to 2020-21, SC 45-day school
  reports 2024-25 and 2025-26, DE statewide charts 2016-17 to 2022-23, and
  WY registry report cards 2018 to 2023 (first DE and WY output). pdftools
  and qpdf are in renv.lock and libpoppler-cpp-dev in the build workflow.
- `scripts/generate_measure_info.R` now unions the columns of every
  `standard/*.csv.gz`, emits a `_catalog` block per state, and carries the
  new measure names.
