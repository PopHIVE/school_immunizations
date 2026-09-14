# Source status

Checked 2026-09-14 11:58 UTC by `scripts/check_sources.R`. Do not edit by hand.

Status values: `ok`, `new files` (posted upstream, not in raw/), `upstream changed` (ETag or Last-Modified differs from the last fetch), `stale` (the school year that should be available by now has not been ingested), `unreachable`, `pattern matched nothing`, `not automatable` (manual, request or dashboard sources; staleness only), `unverifiable` (site blocks this client).

| State | Source | Access | Status | Expected year | Latest ingested | Detail |
|---|---|---|---|---|---|---|
| AK | vactrak_quarterly | manual | not automatable | 2025 |  |  |
| AL | school_entry_survey | index_page | stale | 2025 | 2020 | 12 unmatched data link(s) on page, e.g. organizationalchart.pdf; guidetoservices.pdf; schoolsurveypacket.pdf \| expected school year 2025, latest ingested 2020 |
| AL | exemption_request_workbooks | manual | stale | 2025 | 2024 | expected school year 2025, latest ingested 2024 |
| AR | act676_district_workbook | request | not automatable | 2025 |  |  |
| AZ | idr_report_stats | dashboard | not automatable | 2025 |  |  |
| CA | cdph_report_tables | static | unverifiable | 2025 | 2024 | site blocks datacenter IPs; not checked from CI \| expected school year 2025, latest ingested 2024 |
| CA | exemption_workbook | manual | not automatable |  |  |  |
| CO | cdphe_county_csv | arcgis | ok | 2025 | 2025 |  |
| CO | cdphe_statewide_csv | arcgis | ok | 2025 | 2025 |  |
| CO | cdphe_district_csv | arcgis | ok | 2025 | 2025 |  |
| CO | cdphe_facility_2017_2022_csv | arcgis | stale | 2025 | 2022 | expected school year 2025, latest ingested 2022 |
| CO | cdphe_facility_2023_2025_csv | arcgis | ok | 2025 | 2025 |  |
| CT | socrata_county | socrata | ok | 2025 | 2025 |  |
| CT | socrata_school_k | socrata | ok | 2025 | 2025 |  |
| CT | socrata_school_7 | socrata | ok | 2025 | 2025 |  |
| CT | socrata_school_exempt_all | socrata | ok | 2025 | 2025 |  |
| CT | all_grades_workbook | manual | not automatable |  | 2024 |  |
| DC | dc_health_mmr_by_school | request | unverifiable | 2025 |  | site blocks datacenter IPs; not checked from CI |
| DE | dph_school_survey | index_page | stale | 2025 | 2022 | 5 unmatched data link(s) on page, e.g. varicellaimmunitystatement.pdf; schoolvaccinationmedicalexemptionform.pdf; supplementalschoolvaccinationmedicalexemptionform.pdf \| expected school year 2025, latest ingested 2022 |
| FL | flhealthcharts_kindergarten | report_viewer | ok | 2025 | 2025 |  |
| FL | scraped_exemptions | manual | not automatable |  |  |  |
| GA | dph_no_source | request | not automatable | 2025 |  |  |
| HI | doh_exemption_reports | index_page | ok | 2024 | 2024 | 12 unmatched data link(s) on page, e.g. 11-157.pdf; Immunization_Examination_Req_Report_for_School_Year_24_25.pdf; Immunization_Examination_Req_Report_for_School_Year_23_24.pdf |
| IA | audit_exemption_csvs | manual | stale | 2025 | 2024 | expected school year 2025, latest ingested 2024 |
| IA | kindergarten_summary_pdf | index_page | ok | 2025 | 2025 | 19 unmatched data link(s) on page, e.g. K-12%20Immunization%20Summary%202025-26.pdf; K-12%20Grade%20Summary%20by%20School%202025-26.pdf; K-12%20Grade%20Summary%20by%20School%202024-25.pdf |
| ID | dhw_data_request | request | not automatable | 2025 |  |  |
| IL | isbe_public_use_files | manual | not automatable | 2025 |  |  |
| IN | idoh_ckan | ckan | ok | 2025 | 2025 |  |
| KS | kdhe_kindergarten_data | manual | not automatable | 2025 |  |  |
| KY | kdph_workbook | manual | not automatable | 2025 |  |  |
| KY | annual_county_report_pdf | manual | not automatable | 2025 |  |  |
| LA | ldh_parish_workbook | manual | not automatable | 2025 |  |  |
| MA | mass_gov_current | index_page | unverifiable | 2025 |  | site blocks datacenter IPs; not checked from CI |
| MA | mass_gov_archive | index_page | unverifiable | 2025 |  | site blocks datacenter IPs; not checked from CI |
| MD | mdh_by_school | index_page | ok | 2025 | 2025 | 5 unmatched data link(s) on page, e.g. ImmuNet_Reminder-Recall-Request.pdf; 2018-2019-Maryland-Schools-Kindergarten.xlsx; 2017-2018-Maryland-Schools-Kindergarten.xlsx |
| ME | mecdc_workbooks | index_page | stale | 2025 | 2024 | 58 unmatched data link(s) on page, e.g. 2025%20Healthcare%20Facility%20Immunization%20Assessment%20Report%20FINAL_3.pdf; 2024%20HCW%20Immunization%20Assessment%20FINAL.pdf; 2023%20Healthcare%20Worker%20Immunization%20Assessment%20FINAL%20DRAFT.pdf \| expected school year 2025, latest ingested 2024 |
| MI | mdhhs_building_files | index_page | stale | 2025 | 2024 | 8 unmatched data link(s) on page, e.g. MDHHS-Dept-Overview.pdf; Nonmedical-Waiver-Frequently-Asked-Questions.pdf; School-Summary_ADA_2024.pdf \| expected school year 2025, latest ingested 2024 |
| MN | mdh_county_workbooks | index_page | ok | 2025 | 2025 | 36 unmatched data link(s) on page, e.g. aisrsumm2526.pdf; kdistrict2526.xlsx; kschool2526.xlsx |
| MO | dhss_dashboard_workbook | dashboard | not automatable | 2025 |  |  |
| MS | msdh_exemption_records | manual | not automatable | 2025 |  |  |
| MS | compliance_report_pdf | static | ok | 2025 |  |  |
| MT | dphhs_reports | manual | not automatable |  |  |  |
| NC | dph_dashboard | dashboard | not automatable | 2025 |  |  |
| ND | hhs_dashboard_request | dashboard | not automatable | 2025 |  |  |
| NE | dhhs_no_source | request | not automatable | 2024 |  |  |
| NH | dhhs_annual_report | manual | not automatable | 2025 |  |  |
| NJ | njdoh_status_reports | manual | not automatable | 2025 |  |  |
| NM | nmdoh_exemption_csvs | manual | not automatable | 2025 |  |  |
| NV | dpbh_mmr_workbook | request | not automatable | 2025 |  |  |
| NY | socrata_2019_on | socrata | stale | 2025 | 2024 | expected school year 2025, latest ingested 2024 |
| NY | socrata_2012_2018 | socrata | ok |  | 2018 |  |
| OH | dataohio_dashboard | dashboard | not automatable | 2025 |  |  |
| OK | osdh_county_tables | index_page | ok | 2025 | 2025 | 17 unmatched data link(s) on page, e.g. Title-70-Oklahoma-School-Immunization-Law.pdf; immunization-regulations.pdf; Guide%20To%20Immunization%20Requirements.pdf |
| OK | osdh_school_level | index_page | ok | 2025 | 2025 | 21 unmatched data link(s) on page, e.g. Title-70-Oklahoma-School-Immunization-Law.pdf; immunization-regulations.pdf; Guide%20To%20Immunization%20Requirements.pdf |
| OR | oha_k12_workbook | static | stale | 2025 | 2024 | expected school year 2025, latest ingested 2024 |
| PA | padoh_county_surveys | index_page | ok | 2025 | 2025 | 19 unmatched data link(s) on page, e.g. School%20Immunization%20Survey%20Summary%20for%20PA%202025-2026.xls; School%20Immunization%20Survey%20Summary%20for%20PA%202024-2025.xls; School%20Immunization%20Survey%20Summary%20for%20Pa%202023-2024.xlsx |
| RI | ridoh_supplied | request | not automatable | 2025 |  |  |
| SC | dph_45_day_reports | index_page | ok | 2025 | 2025 | 3 unmatched data link(s) on page, e.g. 00029-ENG-CR.pdf; R.60-8.pdf; CR-011762.pdf |
| SC | dph_county_page | dashboard | stale | 2025 | 2022 | expected school year 2025, latest ingested 2022 |
| SD | doh_data_request | request | not automatable | 2025 |  |  |
| TN | tdh_mmr_county | static | ok |  | 2024 |  |
| TN | kindergarten_compliance_pdf | index_page | stale | 2025 | 2024 | 174 unmatched data link(s) on page, e.g. 2024-Immunization-Status-Survey-of-24-Month-Old-Children.pdf; 2023_Immunization_Status_Survey_of_24-Month-Old_Children.pdf; 2022-24-Month-Old-Survey.pdf \| expected school year 2025, latest ingested 2024 |
| TN | kindergarten_school_reports_pdf | index_page | ok |  | 2018 | 176 unmatched data link(s) on page, e.g. 2024-Immunization-Status-Survey-of-24-Month-Old-Children.pdf; 2023_Immunization_Status_Survey_of_24-Month-Old_Children.pdf; 2022-24-Month-Old-Survey.pdf |
| TN | kindergarten_survey_csv | static | ok |  | 2021 |  |
| TX | dshs_coverage_workbooks | index_page | ok | 2025 | 2025 | 7 unmatched data link(s) on page, e.g. 2025-2026-annual-report-of-immunization-status-of-students.pdf; 2024-2025_Annual_Report_of_Immunization_Status_of_Students.pdf; 2023-2024_Annual_Report_of_Immunization_Status_of_Students.pdf |
| TX | dshs_conscientious_exemptions | index_page | ok | 2025 | 2025 | 8 unmatched data link(s) on page, e.g. 2025-2026-conscientious-exemptions-by-school-district.pdf; 2024-2025_K-12_Conscientious_Exemptions_by_District.pdf; 2023-2024_K-12_Conscientious_Exemptions_by_District.pdf |
| UT | udhhs_exemption_workbook | request | not automatable | 2025 |  |  |
| VA | vdh_sis_workbook | manual | not automatable | 2025 |  |  |
| VT | vdh_county_workbook | request | not automatable | 2025 |  |  |
| WA | doh_school_year_workbooks | index_page | ok | 2025 | 2025 | 11 unmatched data link(s) on page, e.g. 3481157ImmunizationSchoolRprting.pdf; 2016-2017SchoolBuilding.xlsx; 2017-2018SchoolBuilding.xlsx |
| WA | doh_request_workbook | manual | not automatable |  |  |  |
| WA | socrata_school_datasets | socrata | ok |  | 2016 |  |
| WI | dhs_school_workbooks | manual | stale | 2025 | 2024 | expected school year 2025, latest ingested 2024 |
| WI | wir_mmr_county | static | stale | 2025 | 2024 | expected school year 2025, latest ingested 2024 |
| WI | dhs_arcgis_schools | arcgis | unreachable | 2025 | 2022 | HTTP NA for https://dhsgis.wi.gov/server/rest/services/DHS_IMMZ/School_Immunization_Rates/MapServer?f=pjson \| expected school year 2025, latest ingested 2022 |
| WV | oeps_foia_workbook | request | not automatable | 2025 |  |  |
| WY | wdh_county_report_cards | manual | stale | 2025 | 2023 | expected school year 2025, latest ingested 2023 |
