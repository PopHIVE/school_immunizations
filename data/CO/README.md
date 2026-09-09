# CO

Colorado Department of Public Health and Environment, School and Child Care
Immunization Data.

## Source

The CDPHE ArcGIS MapServer `OPEN_DATA/cdphe_sccidr` (also surfaced on
data.colorado.gov). Five of its tables are ingested, each paged through the
REST query endpoint into its own CSV under `raw/`:

| Table | Content | Raw file | Output |
|---|---|---|---|
| 1 | county | `CDPHE_Colorado_School_and_Child_Care_Immunization_County_Data_.csv` | `data.csv.gz`, `type = "county"` |
| 4 | statewide | `cdphe_sccidr_statewide.csv` | `data.csv.gz`, `type = "state"`, geography 08 |
| 2 | school district | `cdphe_sccidr_district.csv` | `data_districts.csv.gz` |
| 5 | facility, 2017-18 to 2022-23 | `cdphe_sccidr_facility_2017_2022.csv.gz` | `data_schools.csv.gz` |
| 6 | facility, 2023-24 on | `cdphe_sccidr_facility_2023_2025.csv.gz` | `data_schools.csv.gz` |

Layer 0 (county boundaries) and table 3 (college and university data) are
not ingested. The county file reproduces the former hub CSV export row for
row.

All five tables share one long layout: geography, school year
(`2017/2018`), survey type (Child Care/Preschool, Kindergarten, School),
vaccine, metric, `Value_Percent` and a per-vaccine `Enrollment`. The two
facility tables hold 1.6 million rows (230 MB of CSV) between them, so they
are fetched with a `where` clause that keeps only the three metrics the
parser uses (Fully Immunized, Medical Exemption, Nonmedical Exemption) and
stored gzip-compressed (about 4 MB and 2 MB); the download is compared with
the committed copy by content and the `.gz` is rewritten only when it
differs. The other three tables are fetched whole as plain CSV and keep the
In Process, Incomplete Record, No Record and Summary Compliant rows in
`raw/`.

A failed request keeps the committed copy, warns, and is recorded in
`process.json` under `fetch_state`.

## Output

Every file carries the same measures: per-antigen coverage (`rate_<vaccine>`,
CDPHE's "Fully Immunized"), per-antigen medical and non-medical exemption
rates (`rate_<vaccine>_medical_exempt`, `rate_<vaccine>_nonmedical_exempt`)
and the per-antigen denominator `N_<vaccine>_enrolled`. CDPHE reports
enrolment per vaccine (Tdap is assessed against the grades that require it),
so there is no single `N_enrolled`. `grade` is CDPHE's survey type.

`standard/data.csv.gz`: one row per county, grade and school year, 2017-18
to 2025-26 (`type = "county"`), plus the statewide total for each grade and
year (`type = "state"`, geography `08`). The statewide table reports no COVID
figures, so those columns are NA on the state rows. CDPHE's "Unknown" county
bucket is dropped.

`standard/data_districts.csv.gz`: one row per school district, grade
(Kindergarten, School) and school year, with `district_id` and `district`
as published. The district table carries no county; a district whose
facilities all sit in one county in the facility tables takes that county's
FIPS, and the 13 districts whose facilities span more than one county
(Denver County 1, Jefferson County R-1, Charter School Institute, ...) have
`geography` NA.

`standard/data_schools.csv.gz`: one row per facility, grade and school
year, with `school_id`, `school_name`, `district`, `school_type` (Public
School, Private School, Child Care/Preschool) and the county FIPS. Five
online schools published with county "Unknown" have `geography` NA.
`school_id` is CDPHE's facility ID: it identifies a facility within a school
year, but it is not stable across years (838 IDs carry more than one name
across the series, some renames and some different schools), so name,
district, type and county are kept as published each year. Where an ID's
name differs between years only in letter case, the latest year's spelling
is used throughout.

CDPHE publishes every 2022-23 Tdap row twice for Larimer County, Poudre R-1
and the statewide total, once with the figure and once with 0; the figure is
kept.
