# MD

Maryland Department of Health, Center for Immunization, "Percent of
Kindergarteners Vaccinated by School".

## Source

One workbook per school year, 2019-20 to 2025-26, discovered from the
by-school page (see `sources.json`) and kept in `raw/` under the decoded
file name. The separate by-county page publishes PDFs only, so county
figures are computed here from the school rows, enrollment-weighted.

MDH revises posted workbooks in place. The ingest keeps a file once it is
in `raw/` (`if_exists = "skip"`), so a revision is not picked up on its
own. There are two ways to adopt one: delete the raw file, and the next run
refetches that year; or switch the fetch to `if_exists = "replace"`, which
would follow the site for every year (with the live 2023-2024 workbook,
that year would go from about 1,689 rows to 1,128 under the revised
layout). Both rewrite a settled year, so neither is automatic. Two copies
of 2025-26 are in `raw/`; the plainly named one is parsed.

## Output

`standard/data.csv.gz` with a `type` column: `school` rows and `county`
rows, kindergarten only, with per-antigen coverage (DTaP, polio, MMR,
hepatitis B, varicella), religious and medical exemption rates, and
enrollment. The workbooks change layout across years (sheet name,
enrollment column label, percent versus proportion); the parser handles
each.
