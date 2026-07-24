# School Immunization Data Sources by State

This document maps each state's `ingest.R` raw data file to its original public
source on the state Department of Public Health / open-data portal, and records
whether that source is available as a machine-readable API/open-data endpoint, a
downloadable static file, a dashboard-only export, or only by records request.

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

| State | Source | Format / Notes |
|---|---|---|
| PA | https://www.pa.gov/agencies/health/programs/immunizations/rates | Per-year county Excel + PDF, URLs verified — matches raw files |
| MN | https://www.health.state.mn.us/people/immunize/stats/school/index.html | Direct `.xlsx` per year (e.g. `kcounty2324.xlsx`) + CSV — exact match |
| MD | https://health.maryland.gov/phpa/OIDEOR/IMMUN/Pages/Kindergarten_Immunization_Rates_by_School.aspx | Excel by school & county, 2019–2026 |
| MA | https://www.mass.gov/info-details/archive-of-school-immunization-data-and-exemption-rates | Excel (.xls) files by year |
| ME | https://www.maine.gov/dhhs/mecdc/data-reports/immunization | Excel and PDF per year, 2018–2025 (K/7/12) |
| HI | https://health.hawaii.gov/docd/resources/reports/immunization-examination-requirements/ | Mostly PDF; 2024-25 also Excel |
| IA | https://hhs.iowa.gov/about/data-reports/health-disease/immunization/school-child-care-audits | Annual K-12 audit PDFs |
| IL | https://www.isbe.net/Pages/Health-Requirements-Student-Data.aspx | Public-use data files (raw) + IDPH Tableau |
| IN | https://www.in.gov/health/immunization/school-immunization-data/ | Assessment PDFs + CKAN Excel (`hub.mph.in.gov`) |
| KS | https://www.kdhe.ks.gov/2016/Kindergarten-Immunization-Data | Annual PDF reports + coalition dashboard |
| KY | https://www.chfs.ky.gov/agencies/dph/dehp/Pages/immunization.aspx | Annual PDF; full county data by email request |
| LA | https://ldh.la.gov/immunization-program/vaccination-data-resources | LINKS dashboards + parish PDF maps |
| MI | https://www.michigan.gov/en/mdhhs/adult-child-serv/childrenfamilies/Immunizations/Data-Statistics/school-immunization-data | Building-level xlsx/PDF (page blocks bots; offline Jun–Aug 2026 for migration) |
| MS | https://msdh.ms.gov/page/14,0,71,688.html | Annual PDF (religious only after Jul 2023) |
| MT | https://dphhs.mt.gov/publichealth/immunization/childcareandschoolresources | PDF only; collection stopped after 2018-19 (last is 2020) |
| NH | https://www.dhhs.nh.gov/programs-services/disease-prevention/nh-immunization-program/immunization-guidance-schools | Annual PDF per school year |
| TX | https://www.dshs.texas.gov/immunizations/data/school | Annual PDFs 2019–2025; older by request |
| VA | https://www.vdh.virginia.gov/immunization/datamanagement/sisreports/ | PDF compliance summaries, 2018–2024 |
| SC | https://dph.sc.gov/health-wellness/child-teen-health/vaccine-requirements-info/school-vaccination-coverage-data | PDF; religious-exemption 5-yr report covers 2019–2023 |
| OK | https://oklahoma.gov/health/services/personal-health/immunizations.html | Annual PDF + interactive county map |
| AZ | https://apps.azdhs.gov/IDRReportStats | Interactive query tool (no export button → raw CSVs scraped) + companion PDFs |

## 🟡 Dashboard-only (export or scrape; no clean file)

| State | Source | Notes |
|---|---|---|
| FL | https://www.flhealthcharts.gov/charts/CommunicableDiseases/default.aspx | Query-and-export report viewer (disease counts, not exemptions) |
| ID | https://www.gethealthy.dhw.idaho.gov/idaho-school-immunization-report | Dashboard + downloadable report doc (Laserfiche) |
| MO | https://health.mo.gov/living/families/schoolhealth/dashboard.php | Tableau only (launched Dec 2024); no file/API found |
| NC | https://www.dph.ncdhhs.gov/programs/epidemiology/immunization/data/kindergarten-dashboard | Dashboard only; no download/API |
| ND | https://www.hhs.nd.gov/immunizations/coverage-rates | Power BI dashboard; no static file |
| NJ | https://www.nj.gov/health/cd/statistics/imm-status-reports/dashboard_only.shtml | Dashboard w/ data table; no confirmed export/API |
| OH | https://data.ohio.gov/wps/portal/gov/data/view/annual-ohio-kindergarten-immunization-level-assessment | DataOhio dashboard (launched Apr 2026) |
| OR | https://public.tableau.com/app/profile/oregon.immunization.program/viz/OregonSchoolImmunizationandExemptionRates/School-leveldata | Tableau; CSV/crosstab export available |
| SD | https://doh.sd.gov/health-data-reports/data-dashboards/school-immunization-dashboard | Dashboard + per-year PDFs; raw file likely a records request |
| UT | https://immunize.utah.gov/information-for-the-public/utah-statistics/ | Coverage-report PDFs + dashboard |
| VT | https://www.healthvermont.gov/stats/surveillance-reporting-topic/school-vaccination-data | Dashboard only (HIPAA suppression); files by contact |
| WA | https://doh.wa.gov/data-and-statistical-reports/washington-tracking-network-wtn/school-immunization | Dashboards + report tables; raw data by request |
| WI | https://www.dhs.wisconsin.gov/library/collection/p-01892 | Per-year PDFs + ArcGIS web app |

## 🔴 By-request / not publicly downloadable

| State | Source | Notes |
|---|---|---|
| AL | https://www.alabamapublichealth.gov/immunization/school-entry-survey.html | PDF/Caspio only; grade-level data not published → records request or CDC SchoolVaxView |
| AR | https://healthy.arkansas.gov/programs-services/community-family-child-health/immunizations/ | No public district-level file; legislative reports / records request |
| NV | https://www.dpbh.nv.gov/programs/immunizations/school-and-child-care-immunizations/ | No public dataset; 2010–2024 MMR series almost certainly from CDC SchoolVaxView |
| WV | https://oeps.wv.gov/immunizations/Pages/school_coverage_rates.aspx | 2025 exemption counts obtained via FOIA; not published |

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
3. **PDF / dashboard exports:** remaining states (manual or semi-automated).
4. **By-request / FOIA (no automation):** AL, AR, NV, WV — likely CDC SchoolVaxView fallback.

### How the rewired scripts run
CI (`.github/workflows/build.yaml`) runs `scripts/build.R` → `dcf::dcf_build()` daily
on Ubuntu with R 4.4.2, installing `dcf` from `dissc-yale/dcf`. Locally, `dcf` is
only available in the R-4.4 renv library; validation here was done under R 4.3.0 with
the `dcf` process-record calls stubbed, exercising the real download + transform.

### Caveats
Access-type calls for MI, WI, WV, NM, and OH were partly inferred from search
snippets because those pages blocked automated fetching or are JavaScript apps.
Verify the exact file/endpoint before wiring them in.
