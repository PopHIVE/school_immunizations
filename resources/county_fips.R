# Canonical county-label -> FIPS resolution, shared by the state ingest scripts.
#
# State workbooks spell county names inconsistently: upper case ("ADAIR"),
# collapsed punctuation ("St.clair"), naive title case ("Mcleod", "O'brien"),
# spelled-out abbreviations ("Saint Croix"), ampersands ("Lewis & Clark"), and
# the occasional outright typo. Joining those literally against
# resources/all_fips.csv.gz misses, and the ingests used to absorb the miss
# with
#
#     geography = if_else(is.na(geography), state_fips[1], geography)
#
# which silently refiled county rows as state totals -- every one of
# Oklahoma's 77 counties among them.
#
# `join_county_fips()` matches on a normalized key instead, and stops on any
# label it cannot account for. A source that starts spelling a county
# differently now breaks the build loudly rather than corrupting the series.
# Labels that legitimately are not counties have to be declared by the caller:
#   statewide - whole-state rows, assigned the 2-digit state FIPS
#   no_fips   - real sub-state areas with no county FIPS (multi-county health
#               districts, "Unknown"); kept with geography = NA
#   drop      - regexes for footnotes and other non-data rows; removed
#   drop_na   - TRUE to remove rows whose label is missing or blank

county_fips_key <- function(x) {
  key <- tolower(as.character(x))
  key <- gsub("&", " and ", key, fixed = TRUE)
  # County-type suffixes carry no identity, but "city" does: Baltimore city
  # and Baltimore County are different FIPS, as are St. Louis city and
  # St. Louis County, so "city" is deliberately left in place. Longest
  # alternatives first so "City and Borough" wins over "Borough".
  key <- sub(
    "\\s+(city and borough|census area|municipality|municipio|county|parish|borough|district)\\s*$",
    "", key
  )
  key <- gsub("\\bsaint\\b", "st", key)
  key <- gsub("[^a-z0-9]+", " ", key)
  trimws(gsub("\\s+", " ", key))
}

# Genuine upstream misspellings, which no amount of normalization can resolve.
# Keys and values are both normalized keys (see county_fips_key()).
county_fips_aliases <- list(
  # The 2018 and 2019 Wisconsin by-school workbooks misspell Walworth on some
  # rows while spelling it correctly on others, so the two spellings have to
  # fold together before the county aggregate is taken.
  WI = c("walwroth" = "walworth")
)

join_county_fips <- function(data, state_abbr, county_col = "county",
                            statewide = character(),
                            no_fips = character(),
                            drop = character(),
                            drop_na = FALSE,
                            fips_path = "../../resources/all_fips.csv.gz") {
  if (!is.data.frame(data)) {
    stop("`data` must be a data frame.", call. = FALSE)
  }
  if (!county_col %in% names(data)) {
    stop("`", county_col, "` is not a column of `data`.", call. = FALSE)
  }

  ref <- vroom::vroom(fips_path, show_col_types = FALSE, progress = FALSE)
  ref <- ref[ref$state == state_abbr, ]
  if (!nrow(ref)) {
    stop("no rows for state '", state_abbr, "' in ", fips_path, call. = FALSE)
  }

  counties <- ref[nchar(ref$geography) == 5, ]
  state_geography <- unique(ref$geography[nchar(ref$geography) == 2])
  if (length(state_geography) != 1) {
    stop("expected exactly one state-level FIPS for ", state_abbr, ", found ",
         length(state_geography), call. = FALSE)
  }

  ref_key <- county_fips_key(counties$geography_name)
  if (anyDuplicated(ref_key)) {
    stop("county names for ", state_abbr, " normalize ambiguously: ",
         paste(unique(ref_key[duplicated(ref_key)]), collapse = ", "),
         call. = FALSE)
  }

  labels <- as.character(data[[county_col]])
  blank <- is.na(labels) | trimws(labels) == ""

  key <- county_fips_key(labels)
  alias <- county_fips_aliases[[state_abbr]]
  if (!is.null(alias)) {
    aliased <- !blank & key %in% names(alias)
    key[aliased] <- unname(alias[key[aliased]])
  }

  matched <- match(key, ref_key)
  # Second pass ignoring word breaks, so "LaSalle"/"La Salle" and
  # "DeWitt"/"De Witt" resolve without a per-state alias. Verified not to
  # collide for any state in all_fips.csv.gz.
  squashed <- gsub(" ", "", ref_key, fixed = TRUE)
  needs_pass2 <- is.na(matched) & !blank
  matched[needs_pass2] <- match(gsub(" ", "", key[needs_pass2], fixed = TRUE), squashed)
  matched[blank] <- NA_integer_
  geography <- counties$geography[matched]

  # Report counties under their canonical FIPS spelling so the same county is
  # labelled identically no matter how its source workbook cased it; keep the
  # source label for statewide and no-FIPS rows, which have no FIPS name.
  short_name <- sub(
    "\\s+(city and borough|census area|municipality|municipio|county|parish|borough|district)\\s*$",
    "", counties$geography_name, ignore.case = TRUE
  )
  geography_name <- ifelse(is.na(matched), labels, short_name[matched])

  dropped <- rep(FALSE, length(labels))
  for (pattern in drop) {
    dropped <- dropped | (!blank & grepl(pattern, labels, ignore.case = TRUE))
  }
  dropped <- dropped | (blank & drop_na)

  is_statewide <- !dropped & !blank & is.na(geography) &
    key %in% county_fips_key(statewide)
  geography[is_statewide] <- state_geography

  accounted <- dropped | is_statewide | !is.na(geography) |
    (!blank & key %in% county_fips_key(no_fips))

  if (any(!accounted)) {
    bad <- unique(labels[!accounted])
    bad <- ifelse(is.na(bad), "<missing>", substr(gsub("\\s+", " ", bad), 1, 90))
    stop(sprintf(
      paste0("join_county_fips(%s): %d row(s) carry %d label(s) that are not ",
             "%s counties:\n  %s\nFix the parser, or account for each label ",
             "with statewide=, no_fips=, drop= or drop_na=."),
      state_abbr, sum(!accounted), length(bad), state_abbr,
      paste(bad, collapse = "\n  ")
    ), call. = FALSE)
  }

  data$geography <- geography
  data$geography_name <- geography_name
  data[!dropped, , drop = FALSE]
}

# Canonical FIPS spelling of a county label, for states that need to aggregate
# by county before joining. Returns NA for labels that are not counties.
canonical_county_name <- function(x, state_abbr,
                                 fips_path = "../../resources/all_fips.csv.gz") {
  ref <- vroom::vroom(fips_path, show_col_types = FALSE, progress = FALSE)
  counties <- ref[ref$state == state_abbr & nchar(ref$geography) == 5, ]
  short <- sub(
    "\\s+(city and borough|census area|municipality|municipio|county|parish|borough|district)\\s*$",
    "", counties$geography_name, ignore.case = TRUE
  )
  key <- county_fips_key(x)
  alias <- county_fips_aliases[[state_abbr]]
  if (!is.null(alias)) {
    aliased <- key %in% names(alias)
    key[aliased] <- unname(alias[key[aliased]])
  }
  ref_key <- county_fips_key(counties$geography_name)
  matched <- match(key, ref_key)
  pass2 <- is.na(matched)
  matched[pass2] <- match(
    gsub(" ", "", key[pass2], fixed = TRUE),
    gsub(" ", "", ref_key, fixed = TRUE)
  )
  short[matched]
}
