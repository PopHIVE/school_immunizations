# School Immunizations

The goal for this project is to format county-level data on school vaccinations obtained from the states. As a worked example see the Arizona (AZ) project.

## Working on the project

1.  go to ./data/ and find the state you are working on. Open the folder and click the .rproj file to open the project

2.  Save any raw data in the /raw subfolder

3.  Open the ingest.R script. This is where you out the script to clean the data. Import all raw files, process so that the output data should have the following columns :

-   geography: (5 digit fips for county, 2 digit fips for state)
-   geography_name: name of the county, or 'Total' for statewide total
-   time (YYYY-09-01), where YYYY is the 4 digit year for the start of the school year
-   pct_XX the percent of children immunized (0-100). XX should be replaced by the state abbreviation. Numeric variable
-   N_XX the number of children in the population (denominator). Numeric variable
-   vax: the name of the vaccine or exemption category:
    -   "dtap"
    -   "polio"
    -   "mmr"
    -   "hep_b"
    -   "varicella"
    -   "personal_exempt"
    -   "medical_exempt"
    -   "full_exempt"

4.  save the formatted dataset as a compressed csv file in the /standard subfolder
5.  Update the measure_info.json file for the project to incclude descriptions of all variables. I recommend editing the jsons in Microsoft Visual Studio to ensure proper formatting
6.  run dcf::dcf_process("XX") to process individual datasets, substituting the state abbreviation for XX. This should be done withint the state-specific project
7.  run dcf::dcf_build() form the root directory (set working directory to school_immunizations. This builds the whole project
8.  You can see the datasets and their relationships here: <https://github.com/PopHIVE/school_immunizations/blob/main/status.md>
9.  See all processed files at: <https://dissc-yale.github.io/dcf/report/?repo=PopHIVE/school_immunizations>

Other notes:

This is set up as a Data Collection Framework project, initialized with `dcf::dcf_init`.

You can use the `dcf` package to check the source projects:

``` r
dcf::dcf_check_source()
```

And process them:

``` r
dcf::dcf_process()
```
