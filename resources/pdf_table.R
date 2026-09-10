# Tables out of agency PDFs.
#
# Most state immunization reports that exist only as PDF are text PDFs whose
# tables were laid out in columns, and pdftools::pdf_text() reproduces that
# layout line by line with runs of spaces between the columns. That is enough
# for a county table: a data row is a line whose leading text matches the
# label the table uses (a county name) and whose remaining fields are all
# numbers or the markers a report uses for withheld cells. Charts whose
# labels are drawn as text, and tables set with single spaces between
# columns, need the word coordinates from pdftools::pdf_data() instead;
# pdf_words() and pdf_lines() expose those.
#
# Usage, from an ingest:
#   source("../../resources/pdf_table.R")
#   pages <- pdf_find_pages(path, "Summary Report by County")
#   rows  <- pdf_table_rows(path, pages, label = "^[A-Z][A-Za-z .'-]+$",
#                           n_fields = 8, col_names = c("n_coi", ...))
#   stopifnot(nrow(rows) == 99)
# Values come back as character so the ingest can run them through
# parse_rate() / censor_flag() and declare their scale, like every other
# source. pdf_number() is the plain numeric parse for counts.
#
# pdftools needs poppler: libpoppler-cpp-dev on Ubuntu (see
# .github/workflows/build.yaml), brew install poppler on macOS.

if (!requireNamespace("pdftools", quietly = TRUE)) {
  stop("resources/pdf_table.R needs the pdftools package")
}

# A field that is a number or one of the markers reports use in a numeric
# column: "1,234", "97.5%", "-", "N/A", "*", "<5", ">96%", "NR", "n/a".
PDF_NUMERIC_FIELD <- paste0(
  "^\\s*(?:",
  "[<>]?\\s*-?[0-9][0-9,]*(?:\\.[0-9]+)?\\s*%?",     # numbers, bounds, percents
  "|\\.[0-9]+\\s*%?",                                # .5
  "|\\*+|-{1,3}|n/?a|nr\\**|--|s|\\(s\\)",           # withheld markers
  ")\\s*$")

# The pattern is ASCII on purpose: Rscript may run in a C locale, where a
# non-ASCII regex fails. Unicode bound and dash characters in the text are
# folded to their ASCII forms first.
pdf_ascii <- function(x) {
  x <- enc2utf8(as.character(x))
  # written as escapes so the source file itself stays ASCII
  x <- gsub("\u2264", "<", x, fixed = TRUE)   # less-than-or-equal
  x <- gsub("\u2265", ">", x, fixed = TRUE)   # greater-than-or-equal
  x <- gsub("[\u2013\u2014\u2212]", "-", x, perl = TRUE)  # en dash, em dash, minus
  iconv(x, "UTF-8", "ASCII", sub = "?")
}

pdf_is_numeric_field <- function(x) {
  grepl(PDF_NUMERIC_FIELD, pdf_ascii(x), ignore.case = TRUE, perl = TRUE)
}

# Plain numeric parse: strips commas, percent signs and bound markers; the
# markers above become NA. Percent signs are stripped without rescaling, so
# declare the scale in the ingest with parse_rate(from = ...).
pdf_number <- function(x) {
  x <- trimws(pdf_ascii(x))
  x <- gsub("[,%]", "", x)
  x <- sub("^[<>]\\s*", "", x)
  suppressWarnings(as.numeric(x))
}

pdf_text_pages <- function(path) {
  pdftools::pdf_text(path)
}

# Pages (1-based) whose text matches a pattern.
pdf_find_pages <- function(path, pattern, text = NULL, ...) {
  if (is.null(text)) text <- pdf_text_pages(path)
  which(grepl(pattern, text, perl = TRUE, ...))
}

# Split one page of pdf_text() output into lines, then each line into
# fields on runs of two or more spaces. A field that still holds two
# numbers with a single space between them (columns set tight) is split
# again.
pdf_split_line <- function(line) {
  fields <- strsplit(trimws(pdf_ascii(line)), "\\s{2,}", perl = TRUE)[[1]]
  out <- character()
  for (f in fields) {
    parts <- strsplit(f, " ", fixed = TRUE)[[1]]
    if (length(parts) > 1L && all(pdf_is_numeric_field(parts))) {
      out <- c(out, parts)
    } else {
      out <- c(out, f)
    }
  }
  out
}

# Data rows from a column-aligned table.
#
#   label      regex the first field (the row label) must match; use it to
#              keep county rows and drop headers, totals and footnotes.
#   n_fields   number of numeric fields expected after the label, or a vector
#              of the counts allowed (a table whose last columns are blank on
#              some rows). A row with any other count is skipped, which is
#              what happens at the table's header block and at footnotes.
#   col_names  names for the numeric fields (length n_fields); default v1..vn.
#   stop       optional regex; scanning a page stops at the first line that
#              matches it (a "Total" row, a second table on the page).
#   label_words  when the label can be more than one field wide (a two-line
#              county name that pdf_text joins with two spaces), the number
#              of leading fields to join into the label.
#
# Returns a data.frame with page, line, label and the numeric fields, all
# character.
pdf_table_rows <- function(path, pages = NULL, label, n_fields, col_names = NULL,
                           stop = NULL, label_words = 1L, text = NULL) {
  if (is.null(text)) text <- pdf_text_pages(path)
  if (is.null(pages)) pages <- seq_along(text)
  if (is.null(col_names)) col_names <- paste0("v", seq_len(max(n_fields)))
  stopifnot(length(col_names) == max(n_fields))
  out <- list()
  for (p in pages) {
    lines <- strsplit(text[[p]], "\n", fixed = TRUE)[[1]]
    for (i in seq_along(lines)) {
      ln <- lines[i]
      if (!nzchar(trimws(ln))) next
      if (!is.null(stop) && grepl(stop, ln, perl = TRUE)) break
      f <- pdf_split_line(ln)
      if (!(length(f) - label_words) %in% n_fields) next
      lab <- paste(f[seq_len(label_words)], collapse = " ")
      vals <- f[-seq_len(label_words)]
      if (!grepl(label, lab, perl = TRUE)) next
      if (!all(pdf_is_numeric_field(vals))) next
      # a short row (fewer fields than the widest allowed) is padded on the
      # right; the ingest decides what a missing trailing column means
      vals <- c(vals, rep(NA_character_, max(n_fields) - length(vals)))
      row <- c(page = p, line = i, label = lab, stats::setNames(vals, col_names))
      out[[length(out) + 1L]] <- row
    }
  }
  if (!length(out)) {
    # same column types as a non-empty result, so results bind
    empty <- stats::setNames(
      as.data.frame(matrix(character(), 0, 3 + max(n_fields)), stringsAsFactors = FALSE),
      c("page", "line", "label", col_names))
    empty$page <- integer()
    empty$line <- integer()
    return(empty)
  }
  df <- as.data.frame(do.call(rbind, out), stringsAsFactors = FALSE)
  df$page <- as.integer(df$page)
  df$line <- as.integer(df$line)
  rownames(df) <- NULL
  df
}

# ---- word coordinates ---------------------------------------------------------
# For charts with text labels and for tables set with single spaces.

# Words on one page with their box: x, y (top-left, points), width, height,
# text, plus xmid and xend.
pdf_words <- function(path, page) {
  w <- pdftools::pdf_data(path)[[page]]
  w <- as.data.frame(w, stringsAsFactors = FALSE)
  w$xmid <- w$x + w$width / 2
  w$xend <- w$x + w$width
  w[order(w$y, w$x), c("x", "y", "width", "height", "xmid", "xend", "text")]
}

# Group words into lines by vertical position: words whose y is within y_tol
# of each other are one line. Returns a list of data.frames (one per line,
# words in x order) each with a "y" attribute, in page order.
pdf_lines <- function(words, y_tol = 2) {
  if (!nrow(words)) return(list())
  words <- words[order(words$y, words$x), ]
  brk <- c(TRUE, diff(words$y) > y_tol)
  grp <- cumsum(brk)
  lapply(split(words, grp), function(d) {
    d <- d[order(d$x), ]
    attr(d, "y") <- mean(d$y)
    d
  })
}

# Assign each word to the nearest of a set of column positions (the x of
# the header words, say). Returns the index into `cols`.
pdf_nearest_col <- function(x, cols) {
  vapply(x, function(v) which.min(abs(cols - v)), integer(1))
}

# Row-count guard: stop with a clear message when a table has the wrong
# number of rows, which is what a layout change looks like from here.
pdf_expect_rows <- function(df, expected, what = "table") {
  if (nrow(df) != expected) {
    stop(sprintf("%s: expected %d rows, found %d (page(s) %s)", what, expected,
                 nrow(df), paste(unique(df$page), collapse = ",")))
  }
  invisible(df)
}
