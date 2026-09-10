# TX

Texas DSHS, Annual Report of Immunization Status of Students.

## Sources

Two DSHS index pages, both scraped so a new school year arrives on its own
(see `sources.json`):

- Coverage: one workbook per school year and grade (Kindergarten, Seventh
  Grade), 2019-20 onward, with a "Coverage by County" sheet and a "Coverage
  by District" sheet. Kept in `raw/coverage/` under the posted names.
- Conscientious exemptions: a multi-year "Conscientious Exemptions by
  County" workbook reissued each year with the window shifted. The three
  posted issues (2013-2024, 2015-2025, 2016-2026) are kept in
  `raw/exemptions/` and unioned; where a county-year appears in more than
  one, the newest issue wins.

Three coverage files are posted with an `.xls` name but are xlsx inside;
the ingest and `resources/fetch.R` open them by signature.

## Outputs

- `standard/data.csv.gz`: county rows (254 per year and grade) and a
  statewide row (`type = "state"`), grades Kindergarten, 7th grade and K-12.
  Per-antigen coverage 2019-20 to 2025-26 for K and 7th; conscientious
  exemption share 2013-14 to 2025-26 for all three grades. Every rate has
  a `flag_` companion; "NR" cells are `missing`.
- `standard/data_districts.csv.gz`: one row per public district or private
  school and grade, with the same coverage columns. DSHS omits schools with
  five or fewer students in the grade.

## History

The exemption series used to come from three hand-built CSVs. Those had
eleven counties' values misaligned (names re-sorted while values kept the
workbook order), so they were removed and the workbooks are read directly.
