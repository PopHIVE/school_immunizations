# CT

Connecticut Department of Public Health, immunization and exemption rates
from the CT Open Data portal (Socrata) plus one manual workbook.

## Sources

- `8kid-pp5k`, county or county-equivalent immunization and exemption rates
  by school year, grade, vaccine and school type, 2012-13 onward. Pulled
  on every run into `raw/ct_county_immunization_exemption_8kid-pp5k.csv`.
- Three by-school tables (kindergarten, seventh grade, all-grades
  exemptions). Their dataset ids stay the same across years and the portal
  overwrites them in place, so only the current year is online; each year's
  snapshot is kept as `raw/ct_school_<kind>_<start year>.csv`. The ingest
  finds the tables through the Socrata catalog and reads the year from the
  title.
- `raw/CT Vaccine Exemptions 2017-2025_All Grades.xlsx`: DPH's all-grades
  exemption rates by traditional county, downloaded by hand. It is the only
  all-grades county figure CT publishes.

## Outputs

- `standard/data.csv.gz`: county and planning-region rows for Pre-K, K, 7th
  and All Grades, with per-vaccine coverage, religious and medical exemption
  and non-compliance rates.
- `standard/data_7th.csv.gz`, `data_k.csv.gz`, `data_pre_k.csv.gz`,
  `data_all_grades.csv.gz`: the same rows split by grade with each grade's
  empty columns dropped.
- `standard/data_schools.csv.gz`: one row per school and grade from the
  by-school tables, resolved to planning-region FIPS, with `flag_`
  companions ("DS" suppressed, "DNC" did not complete).

## Caveats

CT switched from counties to planning regions around 2022-23; the crosswalk
in `resources/county_fips.R` covers both. The 2024-25 7th-grade statewide
row is short in the source and is carried as published.
