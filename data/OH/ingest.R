# =============================================================================
# OH - MMR Exemption Rate (Kindergarten) by County
# =============================================================================

library(dplyr)
library(readxl)
library(stringr)
library(vroom)

if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

raw_file <- "raw/Ohio % of Students with MMR Exemption.xlsx"
raw_state <- list(hash = tools::md5sum(raw_file))

if (!identical(process$raw_state, raw_state)) {
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5, state == "OH") %>%
    select(geography, geography_name, state)

  raw <- read_excel(raw_file, col_names = FALSE)

  header_row <- which(raw[[1]] == "County")[1]
  header <- raw[header_row, ]
  rate_col <- which(header == "MMR Exemption Rate (%)")[1]

  data <- raw[(header_row + 1):nrow(raw), c(1, rate_col)]
  names(data) <- c("county", "rate")

  data <- data %>%
    filter(!is.na(county), county != "County") %>%
    mutate(
      county = str_to_title(str_trim(county)),
      geography_name = paste0(county, " County"),
      rate = as.numeric(rate),
      time = "12-31-2025",
      grade = "Kindergarten",
      measure = "mmr_exempt_pct"
    ) %>%
    left_join(county_fips_lookup, by = c("geography_name" = "geography_name")) %>%
    filter(state == "OH") %>%
    select(
      time,
      geography = geography,
      geography_name,
      grade,
      pct_mmr_exempt = rate
    )

  scale_rate <- if (max(data$pct_mmr_exempt, na.rm = TRUE) <= 1.5) 100 else 1
  data$pct_mmr_exempt <- data$pct_mmr_exempt * scale_rate

  dir.create("standard", showWarnings = FALSE)
  vroom::vroom_write(data, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
