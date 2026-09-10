# WY

Wyoming Department of Health, county immunization report cards.

## Source

One card per county per calendar year, 2018 to 2023, published as PDF (JPG
for 2018) on the WDH county report card page recorded in `sources.json`.
The cards are built from the Wyoming Immunization Registry (WyIR) and the
Immunization Waiver Database, not from a school survey: the coverage figures
describe resident children by age group. Each measure is printed with the
county figure, the Wyoming figure, the US figure from the National
Immunization Survey (dropped from 2021) and the county's rank among the 23
counties.

health.wyo.gov sits behind Cloudflare bot management and refuses scripted
requests intermittently, so the ingest downloads nothing. `raw/` holds one
workbook per card (137 files), made from the PDFs by PDF-to-Excel conversion
or typed by hand, named `<year> <County>.xlsx`, `<County>_<year>.xlsx` or
`<County> <year>.xlsx`. `raw/2018 Converse (1).pdf` is a browser print of the
JPG card with no text layer, so Converse 2018 is missing from the output
(22 counties that year, 23 in every other).

## Outputs

`standard/data.csv.gz`: one row per county and year (`type = "county"`) and
one statewide row per year (`type = "state"`, geography `56`), 143 rows.
`time` is the card's calendar year dated as the school year that starts in
it (`2020-09-01` for the 2020 card). Wisconsin's registry series dates
calendar year N to the school year ending in N, so the two states are one
year apart in `time` for the same calendar year.

Registry coverage, proportions:

- 19 to 35 months: `rate_series_19_35m` (4:3:1:3:3:1:4),
  `rate_dtap_4dose_19_35m`, `rate_polio_3dose_19_35m`,
  `rate_mmr_1dose_19_35m`, `rate_hib_3dose_19_35m`,
  `rate_hep_b_3dose_19_35m`, `rate_varicella_1dose_19_35m`,
  `rate_pcv_4dose_19_35m`, `rate_hep_a_2dose_19_35m`,
  `rate_rotavirus_2dose_19_35m`.
- School-entry age, 2018 to 2021 only: `rate_series_6y` (5:4:2:3:3:2:4),
  `rate_dtap_5dose_6y`, `rate_mmr_2dose_6y`, `rate_varicella_2dose_6y`. The
  2018 cards label this block "Children (6 years of age)"; the 2019 to 2021
  cards label it "Children (7-years-old)". The 2022 and 2023 cards replace
  it with a kindergarten block (below).
- Adolescents: `rate_menacwy_1dose_13_17y`, `rate_menacwy_2dose_16_18y`,
  `rate_hpv_2dose_13_17y`, `rate_tdap_13_17y`.

Waivers, counts approved in the year from the Immunization Waiver Database:
`N_waivers_under_5y`, `N_waivers_5y_plus` (the same waivers by child's age)
and `N_religious_exempt`, `N_medical_exempt` (the same waivers by type). The
two splits sum to the same total on every card.

`standard/data_kindergarten.csv.gz`: the "Kindergarteners" block of the
2022 and 2023 cards, which the footnote attributes to the annual
Immunization Status Report (the school survey), as `rate_dtap`, `rate_mmr`,
`rate_varicella`, `rate_polio` with `grade = "Kindergarten"`, county and
state rows. The 2022 cards print 5 DTaP, 2 MMR, 2 varicella and 4 polio
doses; the 2023 cards print 4 DTaP, 1 MMR, 1 varicella and 3 polio.

The statewide row carries the Wyoming figure the cards print. Every card in
a year is checked against the others; where one disagrees (Albany 2019 HPV,
three Campbell 2023 kindergarten values, all hand-typed) the figure the
other 22 cards carry is kept and the dissent is logged.

NIS influenza coverage, the adult (65+) measures, provider counts, county
rankings and the 2018 vaccine-preventable disease counts are not taken.

## Caveats

- The workbooks are not uniform conversions. Values sit beside the label,
  inside the label's cell, or two measures to a cell; the parser reads each
  sheet as labelled lines and checks header order, the dose in each label,
  the state figures across cards and the waiver identity rather than
  relying on cell positions.
- The 2020 conversions lost the 4 DTaP, 1 MMR, 3 HepB and 4 PCV rows of
  the 19-35 month block; those four measures are present for 4 counties in
  2020 (the hand-typed cards) and missing for 19.
- The 2021 conversions run the county and state waiver counts together in
  one number ("Religious 15971"); they are split with the state count from
  the hand-typed 2021 cards.
- Six 2018 conversions dropped one county waiver count; where three of the
  four are present the fourth is recovered from the identity above (five
  cards, each a medical count of 1). Johnson 2018 lost two and carries NA
  for `N_waivers_5y_plus` and `N_medical_exempt`.
- 2019 Sweetwater prints "87" for 3 Polio with no percent sign; it is read
  as 87 percent. `BigHorn_2022.xlsx` (hand-typed) does not satisfy the
  waiver identity (4 + 26 against 39 + 1) and is kept as typed.
- `measure_info.json` is a placeholder; the measure names are defined in
  `scripts/generate_measure_info.R`.
