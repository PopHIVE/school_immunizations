# Source status

Checked 2026-09-11 19:30 UTC by `scripts/check_sources.R`. Do not edit by hand.

Status values: `ok`, `new files` (posted upstream, not in raw/), `upstream changed` (ETag or Last-Modified differs from the last fetch), `stale` (the school year that should be available by now has not been ingested), `unreachable`, `pattern matched nothing`, `not automatable` (manual, request or dashboard sources; staleness only), `unverifiable` (site blocks this client).

| State | Source | Access | Status | Expected year | Latest ingested | Detail |
|---|---|---|---|---|---|---|
| CA | cdph_report_tables | static | unverifiable | 2025 | 2024 | site blocks datacenter IPs; not checked from CI \| expected school year 2025, latest ingested 2024 |
| CA | exemption_workbook | manual | not automatable |  |  |  |
| MA | mass_gov_current | index_page | unverifiable | 2025 |  | site blocks datacenter IPs; not checked from CI |
| MA | mass_gov_archive | index_page | unverifiable | 2025 |  | site blocks datacenter IPs; not checked from CI |
| MN | mdh_county_workbooks | index_page | ok | 2025 | 2025 | 36 unmatched data link(s) on page, e.g. aisrsumm2526.pdf; kdistrict2526.xlsx; kschool2526.xlsx |
