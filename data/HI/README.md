# HI

Hawaii Department of Health, immunization and examination requirement
reports by school.

## Source

DOH lists one report per school year on its reports page (see
`sources.json`). Through 2023-24 they are PDFs only; the workbooks in `raw/`
named `Hawaii YYYY-YY Vaccine Exemption.xlsx` were transcribed from them by
hand. From 2024-25 DOH also posts the report as xlsx, which the ingest
discovers and fetches under its own name. There is no 2020-21 report.

## Output

`standard/data.csv.gz` with a `type` column: `school` rows and the four
`county` rows, 2014-15 to 2024-25 without 2020-21. `school_type` is one of
Public, Private, Charter, DHS or Day Care Center in that spelling whichever
case the workbook used (the transcribed files are upper case, DOH's own
file title case); a value outside that set is set to NA and the ingest logs
the count and the values. None occur in the current workbooks. Measures are enrollment and the religious and
medical exemption shares; the official 2024-25 file adds the share with no
record, the share missing one or more immunizations, and the not-up-to-date
total, which is the sum of the four categories.

County rows from the official file are taken as published; earlier county
rows are summed from the transcribed school rows.
