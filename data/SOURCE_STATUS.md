# Source status

Checked 2026-09-14 14:27 UTC by `scripts/check_sources.R`. Do not edit by hand.

Status values: `ok`, `new files` (posted upstream, not in raw/), `upstream changed` (ETag or Last-Modified differs from the last fetch), `stale` (the school year that should be available by now has not been ingested), `unreachable` (an HTTP error), `no response` (the runner could not connect; check from a workstation), `pattern matched nothing`, `not automatable` (manual, request or dashboard sources; staleness only), `unverifiable` (site blocks this client).

| State | Source | Access | Status | Expected year | Latest ingested | Detail |
|---|---|---|---|---|---|---|
| CA | cdph_report_tables | static | unverifiable | 2025 | 2024 | site blocks datacenter IPs; not checked from CI \| expected school year 2025, latest ingested 2024 |
| CA | exemption_workbook | manual | not automatable |  |  |  |
| MN | mdh_county_workbooks | index_page | ok | 2025 | 2025 | 36 unmatched data link(s) on page, e.g. aisrsumm2526.pdf; kdistrict2526.xlsx; kschool2526.xlsx |
| WI | dhs_school_workbooks | manual | stale | 2025 | 2024 | expected school year 2025, latest ingested 2024 |
| WI | wir_mmr_county | static | stale | 2025 | 2024 | expected school year 2025, latest ingested 2024 |
| WI | dhs_arcgis_schools | arcgis | stale | 2025 | 2022 | expected school year 2025, latest ingested 2022 |
