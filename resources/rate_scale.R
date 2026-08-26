# Canonical rate scaling and censoring-marker handling, shared by the state
# ingest scripts.
#
# The standard output expresses every proportion as a RATE in [0, 1], in
# `rate_*` columns. Sources disagree about how they publish one: some give
# percent points ("87", "1.88%"), others give Excel-formatted proportions
# (0.87). Each ingest therefore declares its source scale explicitly --
# `from = "percent"` or `from = "rate"` -- rather than inferring it.
#
# Declaring it is the point. Three ingests used to guess the scale from the
# values, and each way of guessing failed differently:
#
#   * Column-global (VT, RI):  if (max(y, na.rm = TRUE) <= 1) y <- y * 100
#     One out-of-band value disables the conversion for the whole column.
#     Vermont's workbook top-codes 660 cells as ">95%"; parsing those as 95
#     pushed the column max above 1, so every genuine 0.85 stayed unscaled and
#     `pct_dtap` ended up spanning 0.85 to 95 -- two scales in one column.
#
#   * Per-element (AL):        ifelse(x > 1, x, x * 100)
#     Rescales part of a column and not the rest, so 0.98 becomes 98 while
#     1.63 stays 1.63.
#
#   * Inverted (MA):           if (max(v, na.rm = TRUE) > 1.5) v <- v / 100
#     Same column-global fragility, aimed at the opposite scale.
#
# Censoring markers are values, not numbers. "<5", ">95%", "*", "N/A" say the
# cell was suppressed or top-coded, so parse_rate() returns NA for them and
# is_censored() reports which rows they were, letting a caller carry a
# suppressed_flag instead of inventing 5, 95 or x/2.

# A cell is censored if it carries a suppression/top-coding marker instead of a
# measurement. Leading <, > or the unicode forms mean the true value is only
# bounded; asterisk runs, "N/A", "--", "." and "NR" mean it was withheld with no
# indication of which way.
CENSOR_LEFT_PATTERN  <- "^\\s*[<≤]"
CENSOR_RIGHT_PATTERN <- "^\\s*[>≥]"
CENSOR_UNDIRECTED_PATTERN <-
  "^\\s*(?:\\*+|n/?a|-{2,}|\\.|nr)\\s*$"

CENSORING_PATTERN <- paste0(
  CENSOR_LEFT_PATTERN, "|", CENSOR_RIGHT_PATTERN, "|", CENSOR_UNDIRECTED_PATTERN
)

is_censored <- function(x) {
  chr <- trimws(as.character(x))
  !is.na(chr) & chr != "" & grepl(CENSORING_PATTERN, chr, ignore.case = TRUE)
}

# Which way a censored cell is bounded, so a consumer can tell "<5" (the true
# value is BELOW the threshold) from ">95%" (ABOVE it) -- they pull an estimate
# in opposite directions, and collapsing both to a single flag loses that.
#
#   "left"       true value is below the printed threshold  ("<5", "≤5")
#   "right"      true value is above it                     (">95%", "≥95")
#   "undirected" withheld with no bound given               ("*", "N/A", "--")
#   NA           not censored
# The three grepl() calls are all made against the FULL `chr`, not `chr[ok]`.
# Subsetting one side of the `&` and not the other made the two vectors
# different lengths whenever any cell was NA or empty, so R recycled the
# shorter one and the answer landed on the wrong rows -- a column with 4 empty
# cells out of 58 got its "left"/"right" marks scattered across unrelated
# counties. It warned only when the lengths were not an exact multiple, so most
# columns failed silently. grepl() is FALSE for NA, which is why `ok` is
# still needed for nothing but readability here.
censor_direction <- function(x) {
  chr <- trimws(as.character(x))
  out <- rep(NA_character_, length(chr))
  ok <- !is.na(chr) & chr != ""
  out[ok & grepl(CENSOR_LEFT_PATTERN, chr)] <- "left"
  out[ok & grepl(CENSOR_RIGHT_PATTERN, chr)] <- "right"
  und <- ok & grepl(CENSOR_UNDIRECTED_PATTERN, chr, ignore.case = TRUE)
  out[und] <- "undirected"
  out
}

# Why a cell has no number, as a per-measure category rather than a row-level
# flag. censor_direction() answers "which way is it bounded"; this answers "what
# did the source actually print", which is the question you need to decide
# whether a missing cell can be modelled, ignored, or must be treated as a real
# gap:
#
#   ""             the source printed a number -- nothing censored, nothing to
#                  report, so the flag is blank and only the exceptions carry
#                  text. A flag column of mostly-empty cells reads as "these few
#                  rows need attention"; one that spells out "no" 200 times does
#                  not.
#   "suppressed"   withheld to protect a small count (an asterisk run: * ** ***)
#   "missing"      not reported at all ("N/A", "--", ".", "NR")
#   "top_coded"    right-censored, true value ABOVE the printed bound (">95%")
#   "bottom_coded" left-censored, true value BELOW it ("<5")
#   NA             the source cell was empty -- no marker and no number
#
# For the two bounded cases the value column holds the bound the source printed
# (see censor_bound()), not a substitute invented for it -- so the number always
# matches the source, and the flag is what tells you it is a bound rather than a
# measurement.
#
# "suppressed" and "missing" both leave the value NA but are not the same thing:
# a suppressed cell has a measurement behind it that the state chose not to
# print (so the underlying quantity exists and is small), whereas a missing cell
# was never collected. Averaging over the first understates; averaging over the
# second is simply thinner data.
#
# "" and NA are distinct in memory -- a printed number against an empty source
# cell -- but both serialise to an empty CSV field, so the distinction does not
# survive a write. Where it matters, a source with genuinely blank cells should
# map them to "missing" instead.
CENSOR_FLAG_NONE <- ""
SUPPRESSED_PATTERN <- "^\\s*\\*+\\s*$"
MISSING_PATTERN <- "^\\s*(?:n/?a|-{2,}|\\.|nr)\\s*$"

censor_flag <- function(x) {
  chr <- trimws(as.character(x))
  out <- rep(CENSOR_FLAG_NONE, length(chr))
  out[is.na(chr) | chr == ""] <- NA_character_
  ok <- !is.na(chr) & chr != ""
  out[ok & grepl(SUPPRESSED_PATTERN, chr)] <- "suppressed"
  out[ok & grepl(MISSING_PATTERN, chr, ignore.case = TRUE)] <- "missing"
  out[ok & grepl(CENSOR_RIGHT_PATTERN, chr)] <- "top_coded"
  out[ok & grepl(CENSOR_LEFT_PATTERN, chr)] <- "bottom_coded"
  out
}

# One flag per row where several source rows collapse onto one output cell. The
# retained value is the one that survived the summarise, so the marker that
# survives with it is the most informative one: an actual number beats a bound,
# and a bound beats a withheld cell.
CENSOR_FLAG_PRECEDENCE <- c(CENSOR_FLAG_NONE, "top_coded", "bottom_coded",
                            "suppressed", "missing")

combine_censor_flag <- function(x) {
  x <- x[!is.na(x)]
  if (!length(x)) return(NA_character_)
  known <- intersect(CENSOR_FLAG_PRECEDENCE, x)
  if (!length(known)) return(x[[1]])
  known[[1]]
}

# Collapse several censor_direction() vectors into one value per row, for a wide
# frame where one row carries several measures. "left+right" means the row has
# censoring of both kinds.
combine_censor_direction <- function(...) {
  cols <- list(...)
  n <- max(vapply(cols, length, integer(1)))
  vapply(seq_len(n), function(i) {
    v <- unique(stats::na.omit(vapply(cols, function(cc) {
      if (length(cc) == 1L) cc[[1]] else cc[[i]]
    }, character(1))))
    if (!length(v)) return(NA_character_)
    paste(sort(v), collapse = "+")
  }, character(1))
}

# The bound a censored cell states, on the rate scale: "<5" -> 0.05, ">95%" ->
# 0.95. NA for an undirected marker, which states no bound.
#
# The "=?" matters. CENSORING_PATTERN accepts the two-character spellings "<="
# and ">=", so is_censored() calls such a cell bounded -- but the strip below
# used to remove only the "<", leaving "=5.0%", which parses to NA. The cell
# was then reported as bounded with no bound to show for it. Sources that spell
# it "≤" were unaffected, which is why it went unnoticed.
censor_bound <- function(x, from = c("percent", "rate")) {
  from <- match.arg(from)
  chr <- trimws(as.character(x))
  dir <- censor_direction(chr)
  num <- clean_numeric(sub("^\\s*[<>≤≥]=?\\s*", "", chr))
  out <- ifelse(dir %in% c("left", "right"), num, NA_real_)
  if (from == "percent") out <- out / 100
  out
}

# A rate computed from the source's own numerator and denominator.
#
# This is the scale-proof way to get a rate: it is arithmetic on two counts, so
# there is nothing to declare and nothing to guess. Prefer it over parsing a
# published percentage whenever the source prints both counts -- see
# check_rate_against_counts() for using the counts to police a published share.
rate_from_counts <- function(numerator, denominator) {
  num <- clean_numeric(numerator)
  den <- clean_numeric(denominator)
  ifelse(!is.na(den) & den > 0, num / den, NA_real_)
}

# Police a parsed rate against the same quantity computed from counts, and stop
# if they disagree by more than `tol`.
#
# This is what removes the need to document a source's scale. A published share
# read on the wrong scale is out by a factor of 100, so it cannot survive a
# comparison against numerator/denominator; rounding in the published figure
# cannot fail it. Where a source prints counts alongside percentages, this turns
# the scale from an assumption into a checked fact.
check_rate_against_counts <- function(rate, numerator, denominator,
                                      label = "source", tol = 0.02) {
  computed <- rate_from_counts(numerator, denominator)
  cmp <- !is.na(rate) & !is.na(computed)
  if (!any(cmp)) return(invisible(NULL))
  off <- cmp & abs(rate - computed) > tol
  if (any(off)) {
    i <- which(off)[1]
    stop(sprintf(
      paste0("check_rate_against_counts(%s): %d of %d value(s) disagree with ",
             "numerator/denominator by more than %.3g.\n  first: rate=%.6g but ",
             "%.6g/%.6g = %.6g\n  A factor-of-100 gap means the published share ",
             "was read on the wrong scale."),
      label, sum(off), sum(cmp), tol, rate[i],
      clean_numeric(numerator)[i], clean_numeric(denominator)[i], computed[i]),
      call. = FALSE)
  }
  invisible(NULL)
}

# Parse a source column to a rate in [0, 1].
#
#   x    - raw column (character or numeric)
#   from - "percent" if the source publishes percent points (87 means 87%),
#          "rate" if it already publishes a proportion (0.87 means 87%)
#
# Censored cells become NA -- pair with is_censored() for a suppressed_flag.
parse_rate <- function(x, from = c("percent", "rate")) {
  from <- match.arg(from)

  out <- clean_numeric(x)
  if (from == "percent") out <- out / 100
  out
}

# Numeric value of a source cell, with censored cells and formatting removed.
# Parsed with as.numeric() rather than readr::parse_number(): only as.numeric()
# reads the scientific notation readxl produces for small proportions
# ("9.8674939999999992E-3"). parse_number() stops at the "E" and returns 9.87 --
# three orders of magnitude out. It also reads ">95%" as 95, which is how
# Vermont's top-coded cells used to defeat its own rescale.
clean_numeric <- function(x) {
  if (is.numeric(x)) return(as.numeric(x))
  chr <- trimws(as.character(x))
  chr[is_censored(chr)] <- NA_character_
  chr <- gsub(",", "", chr, fixed = TRUE)
  chr <- sub("%\\s*$", "", chr)
  chr <- trimws(chr)
  chr[chr == ""] <- NA_character_
  suppressWarnings(as.numeric(chr))
}

# Decide whether one source file publishes percent points or proportions, for
# the rare source that is inconsistent between files and so cannot be declared
# once. Massachusetts is the case in hand: 25 of its 26 by-county workbooks give
# proportions, and MA_grade7_by_county_2019-2020.xlsx gives percent points.
#
# The decision is made only from VACCINATION COVERAGE values, whose true
# magnitude is known to be a large share -- county-level school coverage does
# not fall near zero. At that magnitude the two scales cannot be confused: a
# coverage figure is at most 1 on the rate scale and at least 50 on the percent
# scale. Anything in between means the assumption does not hold for this file,
# and the caller gets an error rather than a coin flip.
#
# Exemption columns must not be passed in. An exemption column that is genuinely
# below 1 percent is indistinguishable from a proportion, and guessing from one
# is exactly the failure this file exists to prevent. Decide from coverage, then
# apply the answer to every column in the same file.
detect_scale_from_coverage <- function(..., label = "source") {
  v <- unlist(lapply(list(...), clean_numeric), use.names = FALSE)
  v <- v[!is.na(v)]
  if (!length(v)) {
    stop("detect_scale_from_coverage(", label,
         "): no numeric coverage values to decide the scale from.", call. = FALSE)
  }
  mx <- max(v)
  if (mx <= 1.5) return("rate")
  if (mx >= 50) return("percent")
  stop("detect_scale_from_coverage(", label, "): coverage maximum is ", mx,
       ", which is neither a proportion (<= 1.5) nor percent points (>= 50). ",
       "Check the file before assuming a scale.", call. = FALSE)
}

# Convert every pct_* / percent-scaled column of a standard frame into the
# canonical rate_* form.
#
#   from - "percent" (default) or "rate", applied to every pct_* column; or a
#          named character vector to declare columns individually, e.g.
#          c(pct_dtap = "rate", pct_medical_exempt = "percent"). Names not
#          present in `data` are an error, so a renamed source column cannot
#          silently keep its old scale.
#
# Columns are renamed pct_<x> -> rate_<x>. A pre-existing rate_<x> is an error
# rather than being overwritten.
as_rate_columns <- function(data, from = "percent") {
  if (!is.data.frame(data)) {
    stop("`data` must be a data frame.", call. = FALSE)
  }

  pct_cols <- grep("^pct_", names(data), value = TRUE)

  if (!is.null(names(from)) && length(from)) {
    unknown <- setdiff(names(from), names(data))
    if (length(unknown)) {
      stop("as_rate_columns(): declared column(s) not in `data`: ",
           paste(unknown, collapse = ", "), call. = FALSE)
    }
    undeclared <- setdiff(pct_cols, names(from))
    if (length(undeclared)) {
      stop("as_rate_columns(): no scale declared for: ",
           paste(undeclared, collapse = ", "),
           "\nDeclare every pct_ column when passing a named `from`.",
           call. = FALSE)
    }
    scales <- from[pct_cols]
  } else {
    from <- match.arg(from, c("percent", "rate"))
    scales <- setNames(rep(from, length(pct_cols)), pct_cols)
  }

  for (col in pct_cols) {
    target <- sub("^pct_", "rate_", col)
    if (target %in% names(data)) {
      stop("as_rate_columns(): `", target, "` already exists, so renaming `",
           col, "` would overwrite it.", call. = FALSE)
    }
    data[[col]] <- parse_rate(data[[col]], from = scales[[col]])
    names(data)[names(data) == col] <- target
  }

  data
}

# Canonical spelling for the count columns.
#
# Counts are N_<measure>; several ingests emitted n_<measure> instead, which
# left measure_info.json documenting N_hep_a while standard/ carried n_hep_a.
# Case is normalised for every count column, and the denominator's known
# synonyms are folded onto N_enrolled -- the name measure_info.json already
# declares in each of those states.
#
# Deliberately NOT folded in, because they are not the same quantity and rule 1
# applies: n_assessed (students assessed, not enrolled), N_surveyed, N_students,
# total_pop_4_18 (census population aged 4-18, not an enrolment count).
ENROLLED_SYNONYMS <- c("enrollment", "total_enrolled", "n_enroll", "N_enroll")

canonical_count_names <- function(data) {
  nms <- names(data)

  # n_<measure> -> N_<measure>
  lower_n <- grepl("^n_", nms)
  nms[lower_n] <- sub("^n_", "N_", nms[lower_n])

  hit <- tolower(nms) %in% tolower(ENROLLED_SYNONYMS)
  nms[hit] <- "N_enrolled"

  dup <- nms[duplicated(nms)]
  if (length(dup)) {
    stop("canonical_count_names(): renaming would create duplicate column(s): ",
         paste(unique(dup), collapse = ", "), call. = FALSE)
  }

  names(data) <- nms
  data
}

# Automatic scale cross-check, run on every write.
#
# This is the part that does not rely on anyone documenting a source. Wherever a
# frame carries rate_<x> alongside N_<x> and N_enrolled, the rate can be
# recomputed from the two counts and compared. A share read on the wrong scale
# is out by a factor of 100 and cannot survive that comparison, so the whole
# class of bug this file was written for is caught by arithmetic rather than by
# a declaration being correct.
#
# It warns rather than stops, because a legitimately different denominator
# (a rate over students assessed against an enrolment count, say) will disagree
# without anything being wrong. A factor-of-100 gap is reported separately, as
# that is a scale error and nothing else.
audit_rate_scale <- function(data, label = "source", tol = 0.02) {
  if (!"N_enrolled" %in% names(data)) return(invisible(NULL))
  den <- clean_numeric(data[["N_enrolled"]])

  for (rc in grep("^rate_", names(data), value = TRUE)) {
    nc <- sub("^rate_", "N_", rc)
    if (!nc %in% names(data)) next

    rate <- clean_numeric(data[[rc]])
    computed <- rate_from_counts(data[[nc]], den)
    cmp <- !is.na(rate) & !is.na(computed)
    if (!any(cmp)) next

    # The factor-of-100 test needs both sides non-zero: a zero cannot be out by
    # a factor of anything, so a zero rate against a non-zero count is an
    # ordinary disagreement and is left to the tolerance check below.
    both <- cmp & rate > 0 & computed > 0
    ratio <- rate[both] / computed[both]
    scale_off <- sum(ratio > 50 | ratio < 0.02)
    if (scale_off > 0) {
      warning(sprintf(
        paste0("%s: %s is out by roughly a factor of 100 against %s/N_enrolled ",
               "in %d of %d row(s) -- the scale looks wrong."),
        label, rc, nc, scale_off, sum(both)), call. = FALSE)
      next
    }
    off <- sum(abs(rate[cmp] - computed[cmp]) > tol)
    if (off > 0) {
      warning(sprintf(
        paste0("%s: %s disagrees with %s/N_enrolled by more than %.3g in %d of ",
               "%d row(s). Not a scale error, so check whether the published ",
               "share uses a different denominator."),
        label, rc, nc, tol, off, sum(cmp)), call. = FALSE)
    }
  }
  invisible(NULL)
}

# Drop measure columns that are empty for every row.
#
# Most ingests used to stamp a block of placeholders --
#
#     N_dtap = NA_real_, N_polio = NA_real_, ... pct_full_exempt = NA_real_
#
# -- to make each state carry a fixed set of "required" columns. The columns
# advertise a measure the state does not publish, measure_info.json then
# documents them as real, and they beat the real thing downstream:
# scripts/build_all_states_county_standard.R picks the first alias that EXISTS,
# so an empty `rate_full_exempt` won over Colorado's populated
# rate_*_nonmedical_exempt columns and Colorado contributed 1,716 rows of
# nothing.
#
# Only measure columns are considered -- an index column (geography, time,
# grade, ...) that came out empty is a parser failure and should surface as one,
# not be quietly removed.
#
# flag_<measure> is included for the same reason its rate_<measure>/N_<measure>
# counterpart is: a state (CA, VT) that pairs each value column with a
# per-measure censoring flag would otherwise be left with an orphaned,
# entirely-empty flag_<x> column whenever rate_<x>/N_<x> itself got dropped --
# most visibly when a wide multi-cohort file is filtered down to the rows of
# one cohort and re-passed through write_standard() to trim what is empty for
# that subset alone.
drop_empty_measures <- function(data, label = "source") {
  measures <- grep("^(N_|n_|rate_|pct_|total_pop|flag_)", names(data), value = TRUE)
  empty <- measures[vapply(data[measures], function(v) all(is.na(v)), logical(1))]
  if (length(empty)) {
    message(sprintf("%s: dropping %d column(s) with no data: %s",
                    label, length(empty), paste(empty, collapse = ", ")))
    data <- data[, setdiff(names(data), empty), drop = FALSE]
  }
  data
}

# Final write step for a state ingest: convert to rates, canonicalise the count
# names, and write the compressed standard file.
#
# No `state` column is written. It repeated the directory the file already sits
# in, and scripts/build_all_states_county_standard.R labels each state from that
# directory name anyway. `state_name` is still required -- it labels the
# audit_rate_scale() and drop_empty_measures() messages.
#
# delim = "," is passed explicitly because vroom_write() defaults to tab, which
# would produce a tab-separated file named .csv.gz.
write_standard <- function(data, state_name, path = "./standard/data.csv.gz",
                           from = "percent") {
  data <- as_rate_columns(data, from = from)
  data <- canonical_count_names(data)
  audit_rate_scale(data, state_name)
  data <- drop_empty_measures(data, state_name)
  data <- data[, setdiff(names(data), "state"), drop = FALSE]
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  vroom::vroom_write(data, path, delim = ",")
  invisible(data)
}
