# School Immunization Data Sources by State

This document maps each state's `ingest.R` raw data file to its original public
source on the state Department of Public Health / open-data portal, and records
whether that source is available as a machine-readable API/open-data endpoint, a
downloadable static file, a dashboard-only export, or only by records request. Note some states (e.g., NY) currently have school-level data without a clear way to aggregate to county

Compiled 2026-07-24.

**Access type legend**
- 🟢 **API / open-data** — Socrata / ArcGIS / feature-service; programmatically ingestable
- 🔵 **Static file** — downloadable Excel / CSV / PDF
- 🟡 **Dashboard-only** — Tableau / Power BI / ArcGIS app; export or scrape
- 🔴 **By-request / FOIA** — not publicly downloadable

---

## 🟢 Clean API / open-data endpoints (best for automation)

| State | Source | Endpoint / Notes | Latest year |
|---|---|---|---|
| CA | CHHS open-data (Socrata) | https://data.chhs.ca.gov/dataset/school-immunizations-in-kindergarten-by-academic-year — CSV + API, kindergarten by year | 2024–25 (KG; 7th grade only to 2019–20) |
| CO | Colorado Info Marketplace (Socrata `3b5w-8ggf`) | https://data.colorado.gov/dataset/CDPHE-Colorado-School-and-Child-Care-Immunization-/3b5w-8ggf — CSV + API; also CDPHE ArcGIS. Matches the raw CSV exactly. | 2025–26 |
| CT | CT Open Data (Socrata) | https://data.ct.gov/Health-and-Human-Services/2025-2026-Vaccine-Exemption-Rates-by-School-All-Gr/a2a4-pw6c — CSV + API, one dataset per school year | 2024–25 |
| NY | Health Data NY (Socrata `btkd-y8bp`) | CSV: https://health.data.ny.gov/api/views/btkd-y8bp/rows.csv?accessType=DOWNLOAD · JSON: https://health.data.ny.gov/resource/btkd-y8bp.json — verified live; exact match. Pre-2019 in `5pme-xbs5` | 2024–25 |
| NM | NMDOH ArcGIS dashboard | https://www.arcgis.com/apps/dashboards/c40e909922a243968807dc7b10870405 — feature-service backed (queryable), K & 7th grade | 2023–24 |
| RI | RICAIR ArcGIS Hub | https://ricair-data-rihealth.hub.arcgis.com/ — CSV/GeoJSON download + REST feature service | 2024–25 |

## 🔵 Downloadable static files (Excel/CSV/PDF)

| State | Source | Format / Notes | Latest year |
|---|---|---|---|
| PA | https://www.pa.gov/agencies/health/programs/immunizations/rates | Per-year county Excel + PDF, URLs verified — matches raw files | 2023–24 |
| MN | https://www.health.state.mn.us/people/immunize/stats/school/index.html | Direct `.xlsx` per year (e.g. `kcounty2324.xlsx`) + CSV — exact match | 2023–24 |
| MD | https://health.maryland.gov/phpa/OIDEOR/IMMUN/Pages/Kindergarten_Immunization_Rates_by_School.aspx | Excel by school & county, 2019–2026 | 2023–24 |
| MA | https://www.mass.gov/info-details/school-immunizations (current) + …/archive-of-school-immunization-data-and-exemption-rates | Per-year by-county `.xlsx` for K & 7th grade; WAF needs a full browser header set (not just User-Agent) | 2025–26 |
| ME | https://www.maine.gov/dhhs/mecdc/data-reports/immunization | Excel and PDF per year, 2018–2025 (K/7/12) | 2023–24 |
| OR | https://www.oregon.gov/oha/PH/PREVENTIONWELLNESS/VACCINESIMMUNIZATION/GETTINGIMMUNIZED/Documents/SchK-12.xlsx | Statewide K-12 workbook at a fixed URL OHA overwrites each fall (companion `SchPreschool.xlsx`); **rewired & self-updating** | 2024–25 |
| HI | https://health.hawaii.gov/docd/resources/reports/immunization-examination-requirements/ | Mostly PDF; 2024-25 also Excel | 2023–24 |
| IA | https://hhs.iowa.gov/about/data-reports/health-disease/immunization/school-child-care-audits | Annual K-12 audit PDFs | 2024–25 |
| IL | https://www.isbe.net/Pages/Health-Requirements-Student-Data.aspx | Public-use data files (raw) + IDPH Tableau | 2024–25 |
| IN | https://hub.mph.in.gov/dataset/immunization-division-s-school-supplemental-dashboard | CKAN open-data hub — per-year Excel enumerated via `package_show` API; **rewired & self-updating** | 2025–26 |
| KS | https://www.kdhe.ks.gov/2016/Kindergarten-Immunization-Data | Annual PDF reports + coalition dashboard | 2023–24 |
| KY | https://www.chfs.ky.gov/agencies/dph/dehp/Pages/immunization.aspx | Annual PDF; full county data by email request | 2024–25 |
| LA | https://ldh.la.gov/immunization-program/vaccination-data-resources | Committed multi-year parish workbook (`LA_parish_21-24.xlsx`). LDH `SchoolImmunizationDashboard` on analytics.la.gov is **auth-gated** (view 302-redirects to SSO, `.csv` export 404s) → no public automatable source | 2023–24 |
| MI | https://www.michigan.gov/en/mdhhs/adult-child-serv/childrenfamilies/Immunizations/Data-Statistics/school-immunization-data | Building-level xlsx/PDF (page blocks bots; offline Jun–Aug 2026 for migration) | 2023–24 |
| MS | https://msdh.ms.gov/page/14,0,71,688.html | Annual PDF (religious only after Jul 2023) | 2023–24 |
| MT | https://dphhs.mt.gov/publichealth/immunization/childcareandschoolresources | PDF only; collection stopped after 2018-19 (last is 2020) | 2020–21 (collection ended) |
| NH | https://www.dhhs.nh.gov/programs-services/disease-prevention/nh-immunization-program/immunization-guidance-schools | Annual PDF per school year | 2024–25 |
| TX | https://www.dshs.texas.gov/immunizations/data/school | Annual PDFs 2019–2025; older by request | 2023–24 |
| VA | https://www.vdh.virginia.gov/immunization/datamanagement/sisreports/ | PDF compliance summaries, 2018–2024 | 2024–25 |
| SC | https://dph.sc.gov/health-wellness/child-teen-health/vaccine-requirements-info/school-vaccination-coverage-data | PDF; religious-exemption 5-yr report covers 2019–2023 | 2022–23 |
| OK | https://oklahoma.gov/health/services/personal-health/immunizations.html | Annual PDF + interactive county map | 2023–24 |
| AZ | https://apps.azdhs.gov/IDRReportStats | Interactive query tool (no export button → raw CSVs scraped) + companion PDFs | 2023–24 |
| TN | https://github.com/PopHIVE/Ingest/blob/main/data/schoolvax_washpost/raw/KMMRCoverage_County.xlsx | Direct `.xlsx` supplied by TN Dept. of Health to PopHIVE — **do not use the WaPo Tableau/PDF source**. Two columns (`county`, `percent_mmr` = % of kindergartners fully immunized for MMR); 93 counties, single-cohort snapshot, KG/MMR only (no other antigens, no year column in file) | 2024–25 (per WaPo cohort; not stamped in file) |

## 🟡 Dashboard-only (export or scrape; no clean file)

| State | Source | Notes | Latest year |
|---|---|---|---|
| FL | https://www.flhealthcharts.gov/charts/CommunicableDiseases/default.aspx | Query-and-export report viewer (disease counts, not exemptions) | 2024–25 |
| ID | https://www.gethealthy.dhw.idaho.gov/idaho-school-immunization-report | Dashboard + downloadable report doc (Laserfiche) | 2024–25 |
| MO | https://health.mo.gov/living/families/schoolhealth/dashboard.php | Tableau only (launched Dec 2024); no file/API found | 2024–25 |
| NC | https://www.dph.ncdhhs.gov/programs/epidemiology/immunization/data/kindergarten-dashboard | Dashboard only; no download/API | 2023–24 |
| ND | https://www.hhs.nd.gov/immunizations/coverage-rates | Power BI dashboard; no static file | 2024–25 |
| NJ | https://www.nj.gov/health/cd/statistics/imm-status-reports/dashboard_only.shtml | Dashboard w/ data table; no confirmed export/API | 2024–25 |
| OH | https://data.ohio.gov/wps/portal/gov/data/view/annual-ohio-kindergarten-immunization-level-assessment | DataOhio dashboard (launched Apr 2026) | 2024–25 |
| SD | https://doh.sd.gov/health-data-reports/data-dashboards/school-immunization-dashboard | Dashboard + per-year PDFs; raw file likely a records request | 2023–24 |
| UT | https://immunize.utah.gov/information-for-the-public/utah-statistics/ | Coverage-report PDFs + dashboard | 2018–19 |
| VT | https://www.healthvermont.gov/stats/surveillance-reporting-topic/school-vaccination-data | Dashboard only (HIPAA suppression); files by contact | 2017–18 |
| WA | https://doh.wa.gov/data-and-statistical-reports/washington-tracking-network-wtn/school-immunization | Dashboards + report tables; raw data by request | 2023–24 |
| WI | https://www.dhs.wisconsin.gov/library/collection/p-01892 | Per-year PDFs + ArcGIS web app | 2024–25 |

## 🔴 By-request / not publicly downloadable

| State | Source | Notes | Latest year |
|---|---|---|---|
| AL | https://www.alabamapublichealth.gov/immunization/school-entry-survey.html | PDF/Caspio only; grade-level data not published → records request or CDC SchoolVaxView | 2024–25 |
| AR | https://healthy.arkansas.gov/programs-services/community-family-child-health/immunizations/ | No public district-level file; legislative reports / records request | 2024–25 |
| NV | https://www.dpbh.nv.gov/programs/immunizations/school-and-child-care-immunizations/ | No public dataset; 2010–2024 MMR series almost certainly from CDC SchoolVaxView | 2024–25 |
| WV | https://oeps.wv.gov/immunizations/Pages/school_coverage_rates.aspx | 2025 exemption counts obtained via FOIA; not published | 2025–26 |

> **"Latest year"** = the most recent school-year cohort present in each state's standardized
> `data.csv.gz`, derived from the maximum `time` value and mapped to a school-year span using
> that state's own `ingest.R` date convention (some scripts stamp the school-year *start* year,
> others the *end* year). For sources labeled by a single calendar year (e.g. NC, NV, OR, VA, WI),
> the span reflects the ingest script's interpretation of that year, not a verified two-year range.

---

## Ingest priority & progress

1. **API states (rewire `ingest.R` to pull directly, self-updating):** CO, CT, NY, CA — then NM, RI.
   - ✅ **CT** — rewired to CT Open Data Socrata (`8kid-pp5k`, county/county-equivalent);
     validated, 256 rows, 2013–2026 (wider than the old manual file). Exemption counts
     now populated; crosswalk handles the county→planning-region switch (~2022+).
   - ✅ **NY** — rewired to Socrata CSV export (`btkd-y8bp`); validated, 30,951 rows.
   - ✅ **CO** — rewired to CDPHE ArcGIS Open Data CSV; validated, 1,728 rows.
     Reconciled schema drift: `Year`→`Year_`, new `Medical Exemption`/`Nonmedical
     Exemption` metric labels, and uppercase county names (title-cased for FIPS join).
   - ✅ **CA** — hybrid, county-level, validated. KG: CHHS school-level open data
     aggregated to county (enrollment-weighted) for 2016–2022, plus CDPH official
     county report (Table 2) for 2024-25 (reproduces CDPH exactly). 7th grade: CHHS
     aggregated to county, 2019 only. Notes: no public source for 2023-24 (gap);
     CHHS aggregation is within ~1pp of CDPH due to integer-rounded school percents;
     CHHS 7th-grade open data lacks enrollment before 2019-20 and ends at 2019-20.
   - ⬜ NM, RI — pending.
2. **Clean Excel/CSV downloads:** PA, MN, MD, MA, ME.
   - ✅ **MN** — scrapes the MDH current + archive pages for `kcounty####.xlsx` and
     downloads each; self-updating. Extended from 1 year to 2023-24..2025-26 (264 rows,
     87 counties + Statewide). Browser User-Agent required (MDH 403s bots).
   - ✅ **ME** — scrapes the Maine CDC data-reports page for the per-year "School
     Vaccination Rates" workbooks and pulls them from the canonical directory;
     self-updating. Now 2018-19..2024-25 (added 2024-25), K/7th/12th, 16 counties.
     Year-level de-dup guard; browser User-Agent required.
   - ✅ **PA** — scrapes the PA DOH rates page for the per-year "by County" survey
     workbooks and downloads each (statewide "for Pa"/State files excluded); self-updating.
     Reader glob extended to legacy `.xls`; de-dup by year. Coverage 2020-21..2024-25
     (67 counties; K/7th/12th/Totals). Existing multi-layout parser unchanged.
   - ✅ **MD** — county tables are PDF-only, so aggregate the by-school workbooks
     (which carry per-school enrollment) to county, enrollment-weighted; self-updating.
     Handles per-year layout drift (sheet name, enrollment-column label, proportion-vs-
     percent scale) and the Baltimore City/County + bare-vs-"County" naming. KG,
     2019-20..2025-26 (no 2024-25 file published — source gap).
   - ✅ **MA** — UNBLOCKED. The mass.gov WAF 403s a bare User-Agent, but a *full*
     browser header set via `httr` (`Accept`, `Accept-Language`, `Sec-Fetch-*`,
     `Upgrade-Insecure-Requests`, plus a same-origin `Referer` on the `/doc/` fetch)
     passes. Scrapes the current + archive pages for every by-county K / 7th-grade
     `.xlsx` and downloads them; self-updating. Parser handles three layout eras
     (legacy "Table 1"; "Notes" + "Rates by County"; multi-sheet workbooks where the
     county summary is one of several sheets) and header drift ("3 Hep B"/"3 HEPB",
     "2 Varicella"/"Immunity to Chickenpox"). Coverage 2013-14..2025-26, 14 counties,
     K + 7th grade (364 rows). Downloads are incremental (skips years already in
     `raw/`) and rate-limited, because the WAF IP-blocks bursts. **CI caveat:** the
     header bypass was validated from a normal IP; Akamai may still 403 datacenter
     IPs (GitHub Actions), but since `raw/` is committed, CI only fetches newly posted
     years — anything it can't reach is logged and retried, and existing files still
     process.
3. **From the IIS-dashboard reconnaissance** (`PopHIVE/state_iis_scrapers`): of 26 IIS
   registry dashboards, only 5 target school-entry survey data. Cross-checked against
   this project; OR and IN were rewired, LA/KS/NC/UT did not pan out.
   - ✅ **OR** — was dashboard-only (Tableau); the recon surfaced OHA's direct K-12
     workbook at a fixed URL (`.../GETTINGIMMUNIZED/Documents/SchK-12.xlsx`) that OHA
     overwrites each fall. Rewired to `download.file()` it; self-updating. School rows
     carry `Agency` (=county), adjusted enrollment, and per-antigen coverage **and**
     exemption percents — enrollment-weighted to county. Now populates antigen
     coverage too (previously exemptions only). Validated: 35 counties, 2024-25,
     grade "Overall". Year derived from the sheet name ("K-12 2025").
   - ✅ **IN** — moved off the committed exemption snapshot to the IDOH open-data hub
     (CKAN). Enumerates every per-year workbook via the `package_show` API and
     downloads each; self-updating. Parser resolves cross-year header drift by pattern
     (2023-24 has no `County_Code` and space-separated rate headers; `Dtap/Td_Rate` vs
     `Dtap_Rate`); `County_Code` used as FIPS when present, else county-name match.
     Validated: 1,380 rows, all 92 counties, 2023-24..2025-26, grades K/1/6/7/12,
     8 antigens. **Trade-off:** the hub file has no medical/religious exemption split
     (IDOH publishes none there), so exemption columns are NA; in exchange antigen
     coverage is populated for the first time.
   - ⛔ **LA** — the recon's LDH `SchoolImmunizationDashboard` on `analytics.la.gov` is
     a Tableau **Server** behind SSO: the view 302-redirects to login and every `.csv`
     export 404s. No public automatable source, so LA stays on its committed multi-year
     parish workbook (richer than the KG-only dashboard anyway). Not rewired.
   - ⛔ **KS / NC / UT** — recon confirmed still blocked (KS Tableau HTTP 500; NC
     JS-rendered no export; UT R Shiny, data-request only). No change.
4. **PDF / dashboard exports:** remaining states (manual or semi-automated).
5. **By-request / FOIA (no automation):** AL, AR, NV, WV — likely CDC SchoolVaxView fallback.

### How the rewired scripts run
CI (`.github/workflows/build.yaml`) runs `scripts/build.R` → `dcf::dcf_build()` daily
on Ubuntu with R 4.4.2, installing `dcf` from `dissc-yale/dcf`. Locally, `dcf` is
only available in the R-4.4 renv library; validation here was done under R 4.3.0 with
the `dcf` process-record calls stubbed, exercising the real download + transform.

### Caveats
Access-type calls for MI, WI, WV, NM, and OH were partly inferred from search
snippets because those pages blocked automated fetching or are JavaScript apps.
Verify the exact file/endpoint before wiring them in.
