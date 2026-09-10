# DE

Delaware Division of Public Health, annual school immunization survey,
kindergarten.

## Source

DPH publishes the survey as two one-page chart PDFs linked from its school
immunizations page (see `sources.json`). Both are discovered from the page
and fetched on every run, because DPH replaces each file in place at a fixed
URL when it adds a year. They are kept in `raw/` under their posted names:

- `School_Survey_Exemption_Rates.pdf`: "Immunization Status of Surveyed
  Kindergarteners", a 100% stacked bar per school year with four labelled
  segments: fully immunized, medical exemption, religious exemption, out of
  compliance. The four sum to 100.
- `School_Survey_Coverage_Rates.pdf`: "Kindergarten Immunization Coverage
  Rates", a cluster of five bars per school year (DTaP, MMR, Polio, Hep B,
  Varicella) with a target line.

The chart labels are text, so the ingest reads them from word coordinates
(`resources/pdf_table.R`, `pdf_words()`): the exemption values by the row
they share with a school-year label, the coverage values by the year cluster
nearest in x and the bar slot within it.

There are no county figures. Delaware has three counties and DPH publishes
statewide charts only.

## Output

`standard/data.csv.gz`: one statewide row per school year (`geography` 10,
`type` state, `grade` Kindergarten), 2016-17 to 2022-23. Measures are
`rate_fully_immunized`, `rate_medical_exempt`, `rate_religious_exempt`,
`rate_out_of_compliance` and per-antigen coverage `rate_dtap`, `rate_mmr`,
`rate_polio`, `rate_hep_b`, `rate_varicella`, each coverage rate with a
`flag_` companion.

## Caveats

- The coverage chart does not label every bar. In 2016-17 and 2017-18 only
  the Polio bar carries a label; the other four antigens are NA for those
  years with flag `missing`. The bars are drawn, so the values exist, but
  the ingest does not read bar geometry.
- The 2017-18 and 2021-22 rows of the exemption chart are identical
  (95.3 / 0.1 / 1.1 / 3.5), as published.
- A layout change (a different label count per row or cluster, a changed
  legend order, a value outside 80-100) stops the run rather than producing
  a misassigned table.
