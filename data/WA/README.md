# WA

Washington State Department of Health, Washington Tracking Network school
immunization data tables.

## Source

DOH posts one workbook per school year, `<YYYY>-<YYYY>SchoolYear.xlsx`,
2016-17 onward, discovered from the data-tables page (see `sources.json`)
and kept in `raw/` under the posted names. Each has State, County and School
District sheets in one long layout: school year, geography, grade, disease
or vaccine, immunization status, count, enrollment, percent.

`raw/Washington Vaccine Exemption.xlsx` is the earlier records-request
workbook. It is no longer parsed; the DOH workbooks cover the same years
with more detail.

Before the workbooks, DOH published the per-school survey results as
Socrata datasets on data.wa.gov, one per school year and cohort
(Kindergarten, Sixth Grade, K-12) for 2014-15, 2015-16 and 2016-17. The
eight dataset ids are listed in `sources.json` (`socrata_school_datasets`)
and fetched once each to `raw/wa_socrata_<start year>_<k|6th|k12>.csv`;
the series is closed. The dataset titled as sixth grade 2014-15 is a copy
of that year's K-12 table and is not fetched.

## Outputs

- `standard/data.csv.gz`: county rows and a statewide row (`type =
  "state"`), grades Kindergarten, 6th grade (through 2019-20), 7th grade
  (from 2020-21) and K-12. For the overall status: complete, conditional,
  out of compliance, exempt and the four exemption reasons, as counts and
  rates. Per antigen: complete count and rate (`N_<vax>`, `rate_<vax>`),
  exempt count and rate, and for 2016-17 to 2018-19 an "incomplete" count
  and rate. Antigens are the individual diseases from 2019-20 (diphtheria,
  tetanus, pertussis, measles, mumps, rubella, hepatitis B, varicella,
  polio) and DT, pertussis, polio, MMR, hepatitis B and varicella before.
- `standard/data_districts.csv.gz`: the same measures per school district.
- `standard/data_schools_2014_2016.csv.gz`: one row per school and cohort
  (`type = "school"`) from the data.wa.gov datasets, 2014-15 to 2016-17,
  with the school name, district, school type (2015-16 and 2016-17
  Kindergarten and 6th grade only) and the county FIPS. Kindergarten and
  6th grade rows carry complete, conditional, out of compliance, exempt and
  the four exemption reasons, and per antigen (DT, pertussis, MMR, polio,
  hepatitis B, varicella) the complete and incomplete counts and rates;
  K-12 rows carry complete, exempt and the reasons, and per antigen the
  exempt count and rate. Schools with no students in the cohort are
  omitted; schools that did not report are kept with no values and
  `flag_complete = "missing"`. Not aggregated to county. There is no 6th
  grade table for 2014-15.

Rates are computed from count and enrollment; the published percent agrees
to machine precision (one decimal of a percent in the data.wa.gov files)
and is only used to check them.

## Caveats

- K-12 conditional and out-of-compliance counts are not reported for
  2016-17 to 2018-19 and are left NA rather than zero.
- A few districts appear more than once per grade in 2016-17 and 2017-18
  (separate reporting units); they are summed.
- Wahkiakum County is absent from the 2019-20 workbook.
- In the data.wa.gov files, two cells carry a count larger than the
  enrollment (Easton School 2014-15 religious membership exemptions,
  Cornerstone Christian School 2015-16 varicella exemptions); they are NA.
  The 2016-17 school rows sum to the workbook's statewide 6th grade row
  exactly and to its Kindergarten and K-12 rows within 0.1 percent.
