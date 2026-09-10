# MI

Michigan Department of Health and Human Services, immunization status by
building.

## Source

MDHHS posts one building-level workbook per cohort (kindergarten, seventh
grade, new entrants) per year on its school immunization data page and links
only the current year. The ingest discovers the current files from the page
(see `sources.json`) and fetches the unlinked 2024 kindergarten and
new-entrants files from their known media paths; the 2024 seventh-grade file
returns 404. Older workbooks in `raw/` were downloaded by hand and have
inconsistent names. The year in a file name is the school-year end, so
"...-2025.xlsx" is 2024-25.

michigan.gov intermittently blocks non-browser clients; when the page
cannot be read the ingest continues on what is committed.

## Output

`standard/data.csv.gz` with a `type` column: `school` rows (building,
district, school type) and `county` rows summed from them, 2018-19 to
2024-25, with enrollment, complete, provisional and incomplete counts,
completion rate, and waiver counts and rates by reason (medical, religious,
philosophical). Grades are Kindergarten and 7th Grade throughout and New
Entrants from 2022-23; 2023-24 has no 7th Grade rows.

## Caveats

- `PROVISIONAL All Grades Immunization Status by Building 2024.xlsx` has one
  sheet per cohort and is kept in `raw/` as a record, but is not parsed: a
  provisional workbook is skipped whenever a final workbook for the same
  end year is on disk, and the final 2024 kindergarten and new-entrants
  files are. It used to be read from its first sheet as grade "All Grades",
  which duplicated the final 2023-24 kindergarten rows.
- `Seventh-Grade-Immunization-Status-by-Building-2024.xlsx` returns 404 on
  the MDHHS media path (checked 2026-09-04), so there are no seventh-grade
  rows for 2023-24. `scripts/check_sources.R` will list the file if MDHHS
  links it.
- `Waiver data by county 2019 - 2023.xlsx` is a combined-period county table
  with no year detail and is not parsed.
