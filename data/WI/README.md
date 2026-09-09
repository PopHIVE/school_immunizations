# WI

Wisconsin Department of Health Services. Three sources, two fetched by
`ingest.R` and one committed by hand (see `sources.json`).

## Sources

- School waiver workbooks: the per-year `School Immunization Rates,
  Wisconsin, YYYY.xlsx` files from the DHS P-01892 collection, in `raw/`.
  The collection page has no stable per-year link, so these are added by
  hand. They are the school rows of the output.
- Registry MMR: `raw/mmr-map-data.xlsx`, county MMR coverage from the
  Wisconsin Immunization Registry by calendar year and age group. DHS
  revises every year in place (all years were restated in 2025 when the
  age bands changed), so the file is always re-requested. These are the
  county rows.
- ArcGIS school and district layers behind the DHS school immunization web
  map, saved as `raw/wi_arcgis_*.csv`. Kept as a snapshot only: the layer
  matches the 2022-23 workbook already in `raw/` and has no county column.

## Output

`standard/data.csv.gz` with a `type` column:

- `school` rows: medical, religious, personal and total waiver rates per
  school and year, 2018-19 to 2024-25, with `flag_` companions for the
  "<5%" style text the workbooks use.
- `county` rows: `rate_mmr_1dose_24m`, `rate_mmr_2dose_6y` and
  `rate_mmr_2dose_6_18y`, 2019 to 2024. These describe resident children in
  the registry, not a school cohort, which is why they are not named
  `rate_mmr`.
