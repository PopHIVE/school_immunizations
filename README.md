# School Immunizations

The goal for this project is to format county-level data on school vaccinations obtained from the states. 


**Publish the granularity the state publishes.** If a source is by school or by
district, keep those rows and add `school_name` (with `district`/`city` where the
source has them) rather than aggregating in the ingest — the combined file rolls
them up to county itself, and it can only do that correctly if it can see the
denominator on each row. Aggregating early is how WI came to report a 95%
county religious-waiver rate: it averaged school percentages with no enrolment
to weight them, so one small school set the county figure.

The output is **wide**: index columns (`geography`, `time`, and any stratum the
source publishes, such as `grade`) plus one column per measure. The vaccine or
exemption category goes in the COLUMN NAME — `rate_mmr`, `rate_medical_exempt`,
`N_full_exempt` — not in a `vax` value column. Categories in use:

    dtap · polio · mmr · hep_b · varicella
    personal_exempt · medical_exempt · religious_exempt · full_exempt

## Data source status by state

Not every state's data comes from an automated pull. `data/DATA_SOURCES.md`
tracks, for every state, the public source, how it's accessed (API, static
download, dashboard-only, or by-request), and the latest school year covered
— check it before starting work on a state. A few things worth knowing up
front:

-   **AK, DE, GA, NE and WY have no data scraped from an API.** AK is a
    downloadable static file; the rest have no automated download at all.
-   **AK, WY and ID's raw files were supplied directly by Gregg's students**,
    not downloaded by `ingest.R` — there is no automated source behind them.
-   **DE, GA and NE have no source identified yet and no data at all**:
    `ingest.R` is an empty stub and `raw/` is empty for all three.
-   **WY has raw data on hand but it isn't ingested yet** — about 140
    per-county-per-year files sit in `raw/`, but `ingest.R` is still the empty
    stub and there is no `standard/data.csv.gz`.
-   **AR only has school-district-level data** — Arkansas publishes no
    county file, so its `standard/data.csv.gz` has district rows only, not
    county rows.

## Other notes

This is set up as a Data Collection Framework project, initialized with `dcf::dcf_init`.

You can use the `dcf` package to check the source projects:

``` r
dcf::dcf_check()
```

And process them:

``` r
dcf::dcf_process()
```
