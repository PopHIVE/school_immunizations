# Check every declared data source without running any parser.
#
# For each entry in data/<ST>/sources.json this script:
#   * confirms the URL still answers (HEAD, falling back to a one-byte GET,
#     with browser headers), and compares ETag / Last-Modified to the value
#     recorded in process.json fetch_state, so an upstream change shows up
#     before the next build;
#   * runs the discovery pattern on index pages and reports files on the
#     page that are not yet in raw/, plus data-file links the pattern does
#     not match, which is how a renamed new year gets noticed;
#   * works out which school year should be available by now from
#     publish_month / publish_lag_years and flags states whose
#     latest_year_ingested is behind;
#   * treats entries with ci_reachable = false (mass.gov behind a WAF) as
#     unverifiable rather than failed, and manual / request entries as
#     staleness-only.
#
# Output: data/SOURCE_STATUS.md (committed) and a JSON file for CI to keep as
# an artifact. Exit status: 0 clean, 1 something is stale or new files were
# found, 2 a hard failure (HTTP error, invalid content, or a pattern that
# matched nothing where it must).
#
# Usage, from the repo root:
#   Rscript scripts/check_sources.R [--states=CO,CT] [--fetch] [--strict]
#                                   [--out=data/SOURCE_STATUS.md]
#                                   [--json=check_sources.json]
# --fetch downloads NEW files into raw/ (validated, no parsing) so they can
# be reviewed. --strict makes staleness a failure too.

suppressPackageStartupMessages({
  library(jsonlite)
})
source("resources/fetch.R")

args <- commandArgs(trailingOnly = TRUE)
opt <- function(name, default = NULL) {
  hit <- grep(paste0("^--", name, "(=|$)"), args, value = TRUE)
  if (!length(hit)) return(default)
  v <- sub(paste0("^--", name, "=?"), "", hit[1])
  if (!nzchar(v)) TRUE else v
}
states_arg <- opt("states")
do_fetch <- isTRUE(opt("fetch", FALSE))
strict <- isTRUE(opt("strict", FALSE))
out_md <- opt("out", "data/SOURCE_STATUS.md")
out_json <- opt("json", "check_sources.json")

state_dirs <- list.dirs("data", recursive = FALSE, full.names = FALSE)
state_dirs <- state_dirs[grepl("^[A-Z]{2}$", state_dirs)]
if (!is.null(states_arg) && !isTRUE(states_arg)) {
  state_dirs <- intersect(state_dirs, strsplit(states_arg, ",")[[1]])
}

today <- Sys.Date()
this_year <- as.integer(format(today, "%Y"))
this_month <- as.integer(format(today, "%m"))

# The school year whose data should be available now. Data for the school
# year starting in calendar year Y is published in publish_month of
# Y + publish_lag_years; before that month it is the previous year's.
expected_year <- function(publish_month, lag) {
  if (is.null(publish_month) || is.na(publish_month)) return(NA_integer_)
  lag <- if (is.null(lag) || is.na(lag)) 1L else as.integer(lag)
  y <- this_year - lag
  if (this_month < publish_month) y <- y - 1L
  as.integer(y)
}

head_check <- function(url, headers = browser_headers()) {
  resp <- tryCatch(httr::HEAD(url, httr::add_headers(.headers = headers), httr::timeout(60)),
                   error = function(e) NULL)
  code <- if (is.null(resp)) NA_integer_ else httr::status_code(resp)
  if (is.na(code) || code >= 400L || code == 405L) {
    resp <- tryCatch(httr::GET(url, httr::add_headers(.headers = c(headers, Range = "bytes=0-0")),
                               httr::timeout(60)),
                     error = function(e) NULL)
    code <- if (is.null(resp)) NA_integer_ else httr::status_code(resp)
  }
  if (is.null(resp)) return(list(ok = FALSE, code = NA_integer_, etag = NA, last_modified = NA))
  h <- httr::headers(resp)
  # the full size: Content-Length on a plain answer, or the total in
  # Content-Range ("bytes 0-0/39368") on a ranged one. Some servers send
  # neither on HEAD, so ask for one byte in that case.
  range_total <- function(h) {
    cr <- h[["content-range"]] %||% ""
    if (grepl("/[0-9]+$", cr)) as.numeric(sub(".*/", "", cr)) else NA_real_
  }
  # Content-Length is the size on the wire: with Content-Encoding it is the
  # compressed size and says nothing about the file
  encoded <- function(h) nzchar(h[["content-encoding"]] %||% "")
  len <- if (identical(code, 206L)) range_total(h) else suppressWarnings(as.numeric(h[["content-length"]] %||% NA))
  if (!is.na(len) && (len == 0 || encoded(h))) len <- NA_real_
  if (is.na(len) && !is.na(code) && code < 400L) {
    r1 <- tryCatch(httr::GET(url, httr::add_headers(.headers = c(headers, Range = "bytes=0-0")),
                             httr::timeout(60)), error = function(e) NULL)
    if (!is.null(r1)) {
      h1 <- httr::headers(r1)
      len <- if (httr::status_code(r1) == 206L) range_total(h1) else suppressWarnings(as.numeric(h1[["content-length"]] %||% NA))
      if (!is.na(len) && (len == 0 || encoded(h1))) len <- NA_real_
    }
  }
  list(ok = !is.na(code) && code < 400L, code = code,
       etag = h[["etag"]] %||% NA_character_,
       last_modified = h[["last-modified"]] %||% NA_character_,
       content_type = h[["content-type"]] %||% NA_character_,
       length = len)
}

# Does a discovered file already exist in raw/? Ingests rename what they
# download (IN drops a prefix, MA builds names from URL slugs), so an exact
# basename match is not enough. A file counts as present when its normalised
# name contains or is contained in a raw file's name, or when every 4-digit
# year in it (and its grade word, if any) appears in one raw file's name.
norm_name <- function(x) tolower(gsub("[^a-z0-9]", "", tolower(x)))
grade_family <- function(x) {
  x <- tolower(x)
  if (grepl("kinder|\\bkg\\b|-k\\b|_k\\b", x)) return("k")
  if (grepl("seventh|7th|grade-?7|grade7", x)) return("7")
  if (grepl("twelfth|12th|grade-?12", x)) return("12")
  if (grepl("sixth|6th|grade-?6", x)) return("6")
  if (grepl("new-?entrant", x)) return("new")
  ""
}
# School years written into file names in whatever form the agency uses:
# "2024-2025", "2024-25", "24-25", "202425", "2425". Returned as four-digit
# years so names that differ only in convention compare equal (Oklahoma went
# from "CountySummaryTable23-24" to "KGSCountyRates202526").
year_tokens <- function(x) {
  x <- tolower(x)
  out <- regmatches(x, gregexpr("20[0-9]{2}", x))[[1]]
  six <- regmatches(x, gregexpr("(?<![0-9])20([0-9]{2})([0-9]{2})(?![0-9])", x, perl = TRUE))[[1]]
  for (s in six) out <- c(out, paste0("20", substr(s, 3, 4)), paste0("20", substr(s, 5, 6)))
  # "2012-13": a four-digit year followed by the two-digit next year
  spans <- regmatches(x, gregexpr("(?<![0-9])20[0-9]{2}[-_/][0-9]{2}(?![0-9])", x, perl = TRUE))[[1]]
  for (sp in spans) {
    a <- as.integer(substr(sp, 3, 4)); b <- as.integer(substr(sp, 6, 7))
    if (!is.na(b) && b == a + 1L) out <- c(out, paste0("20", sprintf("%02d", b)))
  }
  pairs <- regmatches(x, gregexpr("(?<![0-9])([0-9]{2})[-_]?([0-9]{2})(?![0-9])", x, perl = TRUE))[[1]]
  for (p in pairs) {
    a <- as.integer(substr(gsub("[-_]", "", p), 1, 2)); b <- as.integer(substr(gsub("[-_]", "", p), 3, 4))
    if (!is.na(a) && !is.na(b) && b == a + 1L && a >= 5L && a <= 40L) out <- c(out, paste0("20", sprintf("%02d", c(a, b))))
  }
  sort(unique(out))
}

already_have <- function(candidate, raw_files) {
  if (!length(raw_files)) return(FALSE)
  cand <- utils::URLdecode(basename(sub("\\?.*$", "", candidate)))
  slug <- utils::URLdecode(sub("\\?.*$", "", candidate))
  nc <- norm_name(cand)
  nr <- norm_name(raw_files)
  if (any(nzchar(nc) & (grepl(nc, nr, fixed = TRUE) | vapply(nr, function(r) nzchar(r) && grepl(r, nc, fixed = TRUE), logical(1))))) {
    return(TRUE)
  }
  # years come from the file name only; a directory such as /2024/ is the
  # posting year, not the school year
  yrs <- year_tokens(cand)
  if (!length(yrs)) return(FALSE)
  fam <- grade_family(slug)
  # a raw file named by the start year alone ("45_day_report_2025.pdf")
  # still matches a posted "25-26" name: every year in the raw name must be
  # in the candidate's, and the two must start in the same year
  any(vapply(raw_files, function(r) {
    rf <- grade_family(r)
    ry <- year_tokens(r)
    same_years <- all(yrs %in% ry) ||
      (length(ry) > 0L && all(ry %in% yrs) && min(ry) == min(yrs))
    same_years && (fam == "" || rf == "" || rf == fam)
  }, logical(1)))
}

# Collect the warnings a discovery raises so a 403 (the site refused this
# client) can be told apart from a dead page.
discover_quietly <- function(pg, pattern) {
  msgs <- character()
  res <- withCallingHandlers(
    tryCatch(discover_links(pg, pattern, must_find = FALSE), error = function(e) NULL),
    warning = function(w) { msgs <<- c(msgs, conditionMessage(w)); invokeRestart("muffleWarning") })
  attr(res, "warnings") <- msgs
  res
}

results <- list()
hard_fail <- FALSE
soft_flag <- FALSE

for (st in state_dirs) {
  dir <- file.path("data", st)
  src_path <- file.path(dir, "sources.json")
  if (!file.exists(src_path)) {
    results[[length(results) + 1L]] <- list(state = st, id = "(none)", access = "",
      status = "no sources.json", detail = "", expected = NA, latest = NA)
    soft_flag <- TRUE
    next
  }
  sources <- read_sources(src_path)
  process <- tryCatch(dcf::dcf_process_record(file.path(dir, "process.json")),
                      error = function(e) NULL)
  fetch_state <- process$fetch_state
  changed_manifest <- FALSE

  for (i in seq_along(sources)) {
    s <- sources[[i]]
    exp_year <- expected_year(s$publish_month, s$publish_lag_years)
    latest <- suppressWarnings(as.integer(s$latest_year_ingested %||% NA))
    if (!length(latest)) latest <- NA_integer_
    stale <- isTRUE(!is.na(exp_year) && !is.na(latest) && latest < exp_year)
    status <- "ok"
    detail <- character()

    if (isFALSE(s$ci_reachable) && nzchar(Sys.getenv("GITHUB_ACTIONS"))) {
      status <- "unverifiable"
      detail <- c(detail, "site blocks datacenter IPs; not checked from CI")
    } else if (s$access %in% c("manual", "request", "dashboard")) {
      status <- "not automatable"
    } else {
      # 1. the file / endpoint itself
      if (!is.null(s$url) && nzchar(s$url) && s$access %in% c("static", "socrata", "arcgis", "ckan", "report_viewer")) {
        url <- s$url
        if (s$access == "socrata" && !grepl("\\.csv|rows\\.csv", url)) url <- paste0(url, "?$limit=1")
        if (s$access == "arcgis") url <- paste0(sub("/+$", "", url), "?f=pjson")
        if (s$access == "ckan") url <- s$url
        hc <- head_check(url)
        if (!hc$ok && identical(as.integer(hc$code), 403L)) {
          # the site refused this client, which says nothing about the file
          status <- "unverifiable"
          detail <- c(detail, sprintf("HTTP 403 for %s; check from a workstation", url))
          soft_flag <- TRUE
        } else if (!hc$ok && is.na(hc$code)) {
          # no HTTP status at all: the runner could not connect (some state
          # servers drop datacenter traffic; dhsgis.wi.gov did on 2026-09-14
          # while answering a workstation). Says nothing about the file.
          status <- "no response"
          detail <- c(detail, sprintf("no response from %s; check from a workstation", url))
          soft_flag <- TRUE
        } else if (!hc$ok) {
          status <- "unreachable"
          detail <- c(detail, sprintf("HTTP %s for %s", hc$code, url))
          hard_fail <- TRUE
        } else {
          # compare to what the last fetch stored, if the dest is known
          dest_hint <- if (!is.null(s$dest_glob)) {
            f <- Sys.glob(file.path(dir, s$dest_glob))
            if (length(f)) sub(paste0("^", dir, "/"), "", f) else character()
          } else character()
          prev <- NULL
          for (d in dest_hint) if (!is.null(fetch_state[[d]])) { prev <- fetch_state[[d]]; break }
          # a field that came back from process.json as {} or "NA" is absent
          norm_etag <- function(e) if (!fetch_has_value(e)) NA_character_ else gsub('^W/|"', "", e)
          if (!is.null(prev)) {
            prev_etag <- norm_etag(prev$etag)
            prev_lm <- if (fetch_has_value(prev$last_modified)) prev$last_modified else NA_character_
            have_prev <- !is.na(prev_etag) || !is.na(prev_lm)
            # Last-Modified can differ between an agency's origin servers for
            # the same bytes (dhs.wisconsin.gov does this), so a matching size
            # against the recorded download also counts as unchanged
            prev_bytes <- suppressWarnings(as.numeric(prev$bytes %||% NA))
            same <- (!is.na(hc$etag) && !is.na(prev_etag) && identical(norm_etag(hc$etag), prev_etag)) ||
              (!is.na(hc$last_modified) && !is.na(prev_lm) && identical(hc$last_modified, prev_lm)) ||
              (!is.na(hc$length) && !is.na(prev_bytes) && hc$length == prev_bytes)
            # no ETag and no size to compare (a Drupal server that ignores
            # Range requests): for a small file, download it and compare the
            # hash to the recorded one rather than trust Last-Modified
            if (have_prev && !same && (is.na(hc$etag) || is.na(prev_etag)) &&
                (is.na(hc$length) || is.na(prev_bytes)) &&
                fetch_has_value(prev$sha256) && !is.na(prev_bytes) && prev_bytes <= 5e6) {
              tmp <- tempfile()
              probe <- tryCatch(suppressWarnings(fetch_file(url, tmp, type = "any", retries = 1L,
                                                            conditional = FALSE, min_bytes = 1L)),
                                error = function(e) NULL)
              unlink(tmp)
              if (!is.null(probe) && identical(probe$sha256, prev$sha256)) same <- TRUE
            }
            if (have_prev && !same && (!is.na(hc$etag) || !is.na(hc$last_modified))) {
              status <- "upstream changed"
              detail <- c(detail, sprintf("server: %s / %s; recorded: %s / %s",
                                          hc$etag %||% NA, hc$last_modified %||% NA,
                                          prev$etag %||% NA, prev$last_modified %||% NA))
              soft_flag <- TRUE
            }
          }
        }
      }
      # 2. discovery on index pages
      if (s$access %in% c("index_page") && !is.null(s$page_url) && !is.null(s$pattern)) {
        pages <- unique(c(s$page_url, if (!is.null(s$url) && nzchar(s$url)) s$url))
        found <- character(); cands <- character(); page_fail <- character(); blocked <- FALSE
        http_error <- FALSE
        for (pg in pages) {
          l <- discover_quietly(pg, s$pattern)
          if (any(grepl("HTTP 403", attr(l, "warnings")))) blocked <- TRUE
          if (any(grepl("HTTP [0-9]{3}", attr(l, "warnings")))) http_error <- TRUE
          if (is.null(l) || !isTRUE(attr(l, "fetched"))) { page_fail <- c(page_fail, pg); next }
          found <- c(found, l$url); cands <- c(cands, attr(l, "candidates"))
        }
        found <- unique(found); cands <- unique(cands)
        if (length(page_fail) == length(pages)) {
          if (isFALSE(s$ci_reachable) || blocked) {
            status <- "unverifiable"
            detail <- c(detail, if (blocked) "HTTP 403: site refused this client; check from a workstation"
                                else "site blocks this client; check from a workstation")
            soft_flag <- TRUE
          } else if (!http_error) {
            # connection failed with no HTTP status: the runner's network, not
            # the page
            status <- "no response"
            detail <- c(detail, paste("no response from:", paste(page_fail, collapse = ", ")))
            soft_flag <- TRUE
          } else {
            status <- "unreachable"
            detail <- c(detail, paste("page fetch failed:", paste(page_fail, collapse = ", ")))
            hard_fail <- TRUE
          }
        } else {
          have <- basename(list.files(file.path(dir, "raw"), recursive = TRUE))
          # a source stored under one fixed name is overwritten in place by
          # the ingest, so a renamed edition upstream is not a new file
          fixed_dest <- !is.null(s$dest_glob) && !grepl("[*?]", s$dest_glob) &&
            file.exists(file.path(dir, s$dest_glob))
          new_files <- if (fixed_dest) character() else
            found[!vapply(found, already_have, logical(1), raw_files = have)]
          if (!length(found)) {
            status <- "pattern matched nothing"
            hard_fail <- TRUE
          } else if (length(new_files)) {
            status <- "new files"
            shown <- basename(sub("\\?.*$", "", new_files))
            if (length(shown) > 12L) shown <- c(head(shown, 12L), sprintf("and %d more", length(shown) - 12L))
            detail <- c(detail, paste("NEW:", paste(shown, collapse = "; ")))
            soft_flag <- TRUE
            if (do_fetch) {
              for (u in new_files) {
                dest <- file.path(dir, "raw", utils::URLdecode(basename(sub("\\?.*$", "", u))))
                r <- tryCatch(fetch_file(u, dest, type = s$type %||% "auto", if_exists = "skip"),
                              error = function(e) list(status = "failed", error = conditionMessage(e)))
                detail <- c(detail, sprintf("fetched %s: %s", basename(dest), r$status))
              }
            }
          }
          if (length(cands)) {
            detail <- c(detail, sprintf("%d unmatched data link(s) on page, e.g. %s", length(cands),
                                        paste(head(basename(sub("\\?.*$", "", cands)), 3), collapse = "; ")))
          }
        }
      }
      if (s$access == "ckan" && !is.null(s$url)) {
        base <- sub("/api/3/.*$", "", s$url)
        pkg <- sub(".*[?&]id=", "", s$url)
        res <- tryCatch(ckan_resources(base, pkg, s$pattern), error = function(e) NULL)
        if (is.null(res)) {
          status <- "unreachable"; hard_fail <- TRUE
          detail <- c(detail, "package_show failed")
        } else {
          have <- basename(list.files(file.path(dir, "raw"), recursive = TRUE))
          newr <- res$url[!vapply(res$url, already_have, logical(1), raw_files = have)]
          if (length(newr)) {
            status <- "new files"; soft_flag <- TRUE
            detail <- c(detail, paste("NEW:", paste(basename(newr), collapse = "; ")))
          }
        }
      }
    }

    if (stale) {
      if (status == "ok" || status == "not automatable") status <- "stale"
      detail <- c(detail, sprintf("expected school year %d, latest ingested %d", exp_year, latest))
      if (strict) hard_fail <- TRUE else soft_flag <- TRUE
    }

    sources[[i]]$last_check_time <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")
    sources[[i]]$last_check_ok <- !(status %in% c("unreachable", "pattern matched nothing"))
    changed_manifest <- TRUE

    results[[length(results) + 1L]] <- list(
      state = st, id = s$id, access = s$access, status = status,
      detail = paste(detail, collapse = " | "), expected = exp_year, latest = latest
    )
    message(sprintf("%-2s %-28s %-14s %s", st, s$id, status, paste(detail, collapse = " | ")))
  }
  if (changed_manifest) write_sources(sources, src_path)
}

# ---- outputs -----------------------------------------------------------------
df <- do.call(rbind, lapply(results, function(r) data.frame(
  state = r$state, source = r$id, access = r$access, status = r$status,
  expected = ifelse(is.na(r$expected), "", as.character(r$expected)),
  latest = ifelse(is.null(r$latest) || is.na(r$latest), "", as.character(r$latest)),
  detail = r$detail, stringsAsFactors = FALSE)))

jsonlite::write_json(list(checked = format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC"),
                          results = df), out_json, auto_unbox = TRUE, pretty = TRUE)

md_escape <- function(x) gsub("|", "\\|", x, fixed = TRUE)
lines <- c(
  "# Source status",
  "",
  sprintf("Checked %s UTC by `scripts/check_sources.R`. Do not edit by hand.",
          format(Sys.time(), "%Y-%m-%d %H:%M", tz = "UTC")),
  "",
  "Status values: `ok`, `new files` (posted upstream, not in raw/), `upstream changed` (ETag or Last-Modified differs from the last fetch), `stale` (the school year that should be available by now has not been ingested), `unreachable` (an HTTP error), `no response` (the runner could not connect; check from a workstation), `pattern matched nothing`, `not automatable` (manual, request or dashboard sources; staleness only), `unverifiable` (site blocks this client).",
  "",
  "| State | Source | Access | Status | Expected year | Latest ingested | Detail |",
  "|---|---|---|---|---|---|---|",
  sprintf("| %s | %s | %s | %s | %s | %s | %s |", df$state, df$source, df$access,
          df$status, df$expected, df$latest, md_escape(df$detail))
)
writeLines(lines, out_md)

n_hard <- sum(df$status %in% c("unreachable", "pattern matched nothing"))
n_soft <- sum(df$status %in% c("new files", "upstream changed", "stale", "no response"))
message(sprintf("check_sources: %d source(s); %d hard failure(s), %d flag(s)", nrow(df), n_hard, n_soft))
if (hard_fail) quit(status = 2L)
if (soft_flag) quit(status = 1L)
quit(status = 0L)
