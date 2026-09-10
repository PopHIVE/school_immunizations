# Shared download layer for the state ingest scripts.
#
# Every ingest that pulls from the network used to roll its own download, and
# they failed in different ways. Three of them (CO, CT, NY) wrote
# download.file() output straight onto the committed raw file, so a failed
# transfer truncated it -- the incident data/OR/ingest.R documents. Most sent
# no User-Agent and were served a block page as "data". Only MA retried. None
# recorded that a fetch had failed, so a stale snapshot was indistinguishable
# from a fresh one.
#
# This file makes the rules the same everywhere:
#
#   * Download to a temporary file. Validate it for what it claims to be.
#     Only then copy it over the committed file. A failure never touches
#     raw/.
#   * Always send a full browser header set. Several state sites (mass.gov,
#     maine.gov, oregon.gov, health.state.mn.us) block or redirect bare
#     clients, and a block page served with HTTP 200 must be caught by
#     validation, not trusted.
#   * Retry with backoff, then either warn (a committed copy exists, the
#     ingest continues on it) or stop (nothing to fall back on).
#   * Return a record of what happened so the ingest can store it in
#     process.json under `fetch_state`, where scripts/check_sources.R and
#     scripts/build_status.R can see it.
#
# Nothing in here guesses. A response that cannot be validated is a failure,
# and a failure is reported, not absorbed.

FETCH_USER_AGENT <- paste0(
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# The header set MA established as the minimum that passes the mass.gov WAF.
# A bare User-Agent is not enough there; the Sec-Fetch-* and Accept headers
# are what distinguish a browser navigation from a script.
browser_headers <- function(referer = NULL, accept = NULL) {
  # health.wyo.gov answers 403 when the Accept value lists image/avif and
  # image/webp (what Chrome sends), so the default is the shorter form.
  h <- c(
    "User-Agent" = FETCH_USER_AGENT,
    "Accept" = if (is.null(accept)) {
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    } else {
      accept
    },
    "Accept-Language" = "en-US,en;q=0.9",
    "Sec-Fetch-Dest" = "document",
    "Sec-Fetch-Mode" = "navigate",
    "Sec-Fetch-Site" = if (is.null(referer)) "none" else "same-origin",
    "Upgrade-Insecure-Requests" = "1"
  )
  if (!is.null(referer)) h <- c(h, "Referer" = referer)
  h
}

# Phrases that identify a WAF / bot-block page served with HTTP 200. Kept
# short and specific so a real report never trips them.
FETCH_BLOCK_PATTERNS <- c(
  "access denied", "not allowed", "request blocked", "attention required",
  "pardon our interruption", "captcha", "are you a robot", "bot detection",
  "request unsuccessful\\. incapsula", "the request could not be satisfied"
)

# A block page is short and says so in its title or first lines. A real
# page can mention "captcha" in a script far down (pa.gov does), so the
# body text is only consulted when the document is small.
fetch_looks_blocked <- function(text) {
  txt <- tolower(paste(text, collapse = " "))
  title <- regmatches(txt, regexpr("<title[^>]*>[^<]*</title>", txt))
  hit <- function(s) any(vapply(FETCH_BLOCK_PATTERNS, grepl, logical(1), x = s, perl = TRUE))
  if (length(title) && hit(title)) return(TRUE)
  if (nchar(txt) < 30000L) return(hit(txt))
  FALSE
}

# Sniff the file type when the caller passed "auto": the extension is usually
# right, but a few sources serve .xls that is really .xlsx or a CSV export
# with no extension at all.
fetch_guess_type <- function(path) {
  ext <- tolower(tools::file_ext(path))
  switch(ext,
    xlsx = "xlsx", xlsm = "xlsx", xls = "xls",
    csv = "csv", tsv = "tsv", txt = "csv",
    pdf = "pdf", json = "json", html = "html", htm = "html",
    "any"
  )
}

# Validate a downloaded file. Returns NULL when it passes and a one-line
# reason when it does not. Each check answers "is this the thing the source
# publishes", which is the only question a downloader can answer.
fetch_validate <- function(path, type = "auto", min_bytes = 1024L,
                           expect_cols = NULL, validate = NULL) {
  if (!file.exists(path)) return("file was not written")
  size <- file.size(path)
  if (is.na(size) || size < min_bytes) {
    return(sprintf("only %d bytes (minimum %d)", size, min_bytes))
  }
  if (identical(type, "auto")) type <- fetch_guess_type(path)

  head <- tryCatch(readBin(path, "raw", n = 4096L), error = function(e) raw())
  head_txt <- tryCatch(rawToChar(head[head != as.raw(0)]), error = function(e) "")
  # The head of a workbook or PDF is not text; mark it as bytes so the
  # pattern checks below neither warn nor fail on invalid UTF-8.
  Encoding(head_txt) <- "bytes"

  # An HTML body where a binary file was expected is the WAF-page case.
  if (type %in% c("xlsx", "xls", "pdf", "csv", "tsv", "json") &&
      grepl("^\\s*<(!doctype|html)", head_txt, ignore.case = TRUE, useBytes = TRUE)) {
    return("received an HTML page instead of the file")
  }

  problem <- switch(type,
    xlsx = , xls = {
      # readxl picks the reader from the file extension, and some agencies
      # serve xlsx content under a .xls name (Texas DSHS does). Decide from
      # the signature instead: a zip header is xlsx, an OLE header is xls.
      sig <- head[seq_len(min(4L, length(head)))]
      ext <- if (identical(sig, as.raw(c(0x50, 0x4b, 0x03, 0x04)))) "xlsx"
             else if (identical(sig, as.raw(c(0xd0, 0xcf, 0x11, 0xe0)))) "xls"
             else NA_character_
      if (is.na(ext)) {
        "does not start with a workbook signature"
      } else {
        probe <- tempfile(fileext = paste0(".", ext))
        file.copy(path, probe, overwrite = TRUE)
        on.exit(unlink(probe), add = TRUE)
        sh <- tryCatch(readxl::excel_sheets(probe), error = function(e) NULL)
        if (is.null(sh) || !length(sh)) "does not open as a workbook" else NULL
      }
    },
    csv = , tsv = {
      first <- tryCatch(readLines(path, n = 1L, warn = FALSE), error = function(e) character())
      if (!length(first) || !nzchar(first)) {
        "empty first line"
      } else {
        sep <- if (type == "tsv") "\t" else ","
        fields <- gsub('^"|"$', "", trimws(strsplit(first, sep, fixed = TRUE)[[1]]))
        if (length(fields) < 2L) {
          sprintf("header has %d field(s); expected a delimited table", length(fields))
        } else if (!is.null(expect_cols) && !all(expect_cols %in% fields)) {
          sprintf("header lacks column(s): %s",
                  paste(setdiff(expect_cols, fields), collapse = ", "))
        } else {
          NULL
        }
      }
    },
    pdf = if (!identical(rawToChar(head[seq_len(min(5L, length(head)))]), "%PDF-")) "not a PDF" else NULL,
    json = {
      ok <- tryCatch({ jsonlite::fromJSON(path, simplifyVector = FALSE); TRUE },
                     error = function(e) FALSE)
      if (!ok) "not valid JSON" else NULL
    },
    html = {
      body <- tryCatch(paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n"),
                       error = function(e) "")
      if (!grepl("<(!doctype|html|body)", body, ignore.case = TRUE)) {
        "not an HTML document"
      } else if (fetch_looks_blocked(body)) {
        "HTML looks like a bot-block page"
      } else {
        NULL
      }
    },
    NULL
  )
  if (!is.null(problem)) return(problem)

  if (is.function(validate)) {
    msg <- tryCatch({ validate(path); NULL },
                    error = function(e) paste("validator:", conditionMessage(e)))
    if (!is.null(msg)) return(msg)
  }
  NULL
}

fetch_sha256 <- function(path) {
  if (!file.exists(path)) return(NA_character_)
  digest::digest(path, algo = "sha256", file = TRUE)
}

# A content_key for fetch_file(): a hash of every sheet's cell values, read
# as text, so two exports of the same data compare equal even when the file
# bytes differ (generation timestamp, document properties, zip ordering).
workbook_content_key <- function(path) {
  sheets <- readxl::excel_sheets(path)
  cells <- lapply(sheets, function(s) {
    d <- readxl::read_excel(path, sheet = s, col_names = FALSE, col_types = "text",
                            .name_repair = "minimal")
    c(s, dim(d), unlist(d, use.names = FALSE))
  })
  digest::digest(cells, algo = "sha256")
}

# A field that survived a round trip through process.json. dcf writes an NA
# character as {} and an NA integer as the string "NA", and reads them back
# as an empty list and "NA"; those are absent values, not data.
fetch_has_value <- function(v) {
  !is.null(v) && length(v) == 1L && !is.na(v) &&
    !identical(as.character(v), "NA") && nzchar(as.character(v))
}

# Records carry no NA fields, so nothing ambiguous is ever written to
# process.json; a missing field means "unknown".
fetch_record <- function(url, dest, status, http_status = NA_integer_,
                         bytes = NA_real_, sha256 = NA_character_,
                         etag = NA_character_, last_modified = NA_character_,
                         attempts = 0L, error = NA_character_) {
  r <- list(
    url = url, dest = dest, status = status, http_status = http_status,
    bytes = bytes, sha256 = sha256, etag = etag,
    last_modified = last_modified,
    fetched_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    attempts = attempts, error = error
  )
  r[vapply(r, fetch_has_value, logical(1))]
}

# Download one file.
#
#   url          source URL
#   dest         path under raw/ to promote the file to on success
#   type         "xlsx", "xls", "csv", "tsv", "pdf", "json", "html", "any", or
#                "auto" (from the dest extension)
#   headers      named character vector; browser_headers() by default
#   timeout      seconds per attempt
#   retries      attempts before giving up
#   backoff      seconds to sleep after each failed attempt
#   min_bytes    anything smaller is a failure (block pages are tiny)
#   expect_cols  csv/tsv only: header must contain these names
#   validate     optional function(path) that stop()s when the content is wrong
#   content_key  optional function(path) returning one string that identifies
#                the content rather than the bytes. When dest exists and the
#                new download has the same key, it is "unchanged" and dest is
#                left alone. For servers that regenerate a file per request
#                (a report viewer's Excel export carries a new timestamp every
#                time) so that byte hashes differ while the data does not;
#                workbook_content_key() hashes the cell values of a workbook.
#   if_exists    "replace" (default) or "skip": skip when dest already exists
#                and validates, for per-year files that never change once posted
#   conditional  send If-None-Match / If-Modified-Since from `previous` so an
#                unchanged file costs one round trip and no transfer
#   previous     the record stored for this dest in process$fetch_state, if any
#   label        used in messages
#
# Returns a record list. `status` is one of:
#   "updated"    new content promoted to dest
#   "unchanged"  server said 304, or the bytes matched what was already there
#   "skipped"    if_exists = "skip" and dest already validates
#   "failed"     every attempt failed; dest untouched (a warning was raised)
# When every attempt fails and dest does not exist, this stop()s: there is no
# data to fall back on and the ingest cannot proceed.
fetch_file <- function(url, dest, type = "auto",
                       headers = browser_headers(), timeout = 120,
                       retries = 3L, backoff = c(5, 15, 45),
                       min_bytes = 1024L, expect_cols = NULL,
                       validate = NULL, content_key = NULL,
                       if_exists = c("replace", "skip"),
                       conditional = TRUE, previous = NULL,
                       label = basename(dest)) {
  if_exists <- match.arg(if_exists)
  if (identical(type, "auto")) type <- fetch_guess_type(dest)
  dir.create(dirname(dest), showWarnings = FALSE, recursive = TRUE)

  have <- file.exists(dest)
  if (have && if_exists == "skip" &&
      is.null(fetch_validate(dest, type, min_bytes, expect_cols, validate))) {
    return(fetch_record(url, dest, "skipped", bytes = file.size(dest),
                        sha256 = fetch_sha256(dest)))
  }

  hdr <- headers
  if (conditional && have && !is.null(previous)) {
    if (fetch_has_value(previous$etag)) {
      hdr <- c(hdr, "If-None-Match" = previous$etag)
    }
    if (fetch_has_value(previous$last_modified)) {
      hdr <- c(hdr, "If-Modified-Since" = previous$last_modified)
    }
  }

  errors <- character()
  for (attempt in seq_len(max(1L, retries))) {
    tmp <- tempfile(fileext = paste0(".", if (type == "any") "bin" else type))
    resp <- tryCatch(
      httr::GET(url, httr::add_headers(.headers = hdr), httr::timeout(timeout),
                httr::write_disk(tmp, overwrite = TRUE)),
      error = function(e) e
    )
    if (inherits(resp, "error")) {
      errors <- c(errors, sprintf("attempt %d: %s", attempt, conditionMessage(resp)))
      unlink(tmp)
    } else {
      code <- httr::status_code(resp)
      if (code == 304L && have) {
        unlink(tmp)
        return(fetch_record(url, dest, "unchanged", http_status = 304L,
                            bytes = file.size(dest), sha256 = fetch_sha256(dest),
                            etag = if (fetch_has_value(previous$etag)) previous$etag else NA_character_,
                            last_modified = if (fetch_has_value(previous$last_modified)) previous$last_modified else NA_character_,
                            attempts = attempt))
      }
      if (code == 200L) {
        problem <- fetch_validate(tmp, type, min_bytes, expect_cols, validate)
        if (is.null(problem)) {
          new_sha <- fetch_sha256(tmp)
          etag <- httr::headers(resp)[["etag"]]
          lm <- httr::headers(resp)[["last-modified"]]
          same <- have && identical(new_sha, fetch_sha256(dest))
          if (!same && have && is.function(content_key)) {
            same <- tryCatch(identical(content_key(tmp), content_key(dest)),
                             error = function(e) FALSE)
          }
          if (same) {
            unlink(tmp)
            return(fetch_record(url, dest, "unchanged", http_status = 200L,
                                bytes = file.size(dest), sha256 = fetch_sha256(dest),
                                etag = etag %||% NA_character_,
                                last_modified = lm %||% NA_character_,
                                attempts = attempt))
          }
          ok <- file.copy(tmp, dest, overwrite = TRUE)
          unlink(tmp)
          if (!ok) stop("fetch_file(", label, "): could not write ", dest, call. = FALSE)
          message(sprintf("fetch: %s %s (%s bytes)", if (have) "updated" else "fetched",
                          label, format(file.size(dest), big.mark = ",")))
          return(fetch_record(url, dest, "updated", http_status = 200L,
                              bytes = file.size(dest), sha256 = new_sha,
                              etag = etag %||% NA_character_,
                              last_modified = lm %||% NA_character_,
                              attempts = attempt))
        }
        errors <- c(errors, sprintf("attempt %d: HTTP 200 but %s", attempt, problem))
      } else {
        errors <- c(errors, sprintf("attempt %d: HTTP %d", attempt, code))
      }
      unlink(tmp)
    }
    if (attempt < retries) {
      Sys.sleep(backoff[min(attempt, length(backoff))])
    }
  }

  err <- paste(errors, collapse = "; ")
  if (have) {
    warning(sprintf("fetch: %s could not be refreshed from %s (%s); keeping the committed copy",
                    label, url, err), call. = FALSE)
    return(fetch_record(url, dest, "failed", bytes = file.size(dest),
                        sha256 = fetch_sha256(dest), attempts = retries, error = err))
  }
  stop(sprintf("fetch: %s could not be downloaded from %s (%s) and there is no committed copy to fall back on",
               label, url, err), call. = FALSE)
}

`%||%` <- function(a, b) if (is.null(a)) b else a

# Fetch an HTML index page and return the links on it that match `pattern`.
#
# Returns a data frame (url, href, text) of matches. The attribute
# "candidates" carries every other data-file link on the page (xlsx, xls,
# csv, pdf, zip) so scripts/check_sources.R can report a newly posted file
# whose name the pattern does not yet match -- the failure mode where a new
# year silently never arrives.
discover_links <- function(page_url, pattern, base = page_url, exclude = NULL,
                           must_find = TRUE, headers = browser_headers(),
                           timeout = 60, retries = 3L, backoff = c(5, 15, 45)) {
  tmp <- tempfile(fileext = ".html")
  on.exit(unlink(tmp), add = TRUE)
  rec <- tryCatch(
    fetch_file(page_url, tmp, type = "html", headers = headers, timeout = timeout,
               retries = retries, backoff = backoff, min_bytes = 256L,
               conditional = FALSE, label = page_url),
    error = function(e) e
  )
  if (inherits(rec, "error") || !identical(rec$status, "updated")) {
    msg <- if (inherits(rec, "error")) conditionMessage(rec) else rec$error
    if (must_find) stop("discover_links(", page_url, "): ", msg, call. = FALSE)
    warning("discover_links(", page_url, "): ", msg, call. = FALSE)
    out <- data.frame(url = character(), href = character(), text = character(),
                      stringsAsFactors = FALSE)
    attr(out, "candidates") <- character()
    attr(out, "fetched") <- FALSE
    return(out)
  }

  doc <- xml2::read_html(tmp)
  a <- rvest::html_elements(doc, "a[href]")
  href <- xml2::xml_attr(a, "href")
  text <- trimws(rvest::html_text2(a))
  # Some pages (pa.gov) render their file lists from HTML kept inside a
  # JSON attribute or a script, which the DOM parser never sees as anchors.
  # Scan the raw text as well, once as served and once with the entity and
  # backslash escaping undone, and add any href not already found.
  raw_html <- paste(readLines(tmp, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
  unescaped <- gsub("\\\\(\"|/)", "\\1", raw_html)
  unescaped <- gsub("&#34;|&quot;", '"', unescaped)
  unescaped <- gsub("&lt;", "<", gsub("&gt;", ">", unescaped, fixed = TRUE), fixed = TRUE)
  raw_hrefs <- unlist(regmatches(c(raw_html, unescaped),
                                 gregexpr('href="[^"]+"', c(raw_html, unescaped), perl = TRUE)))
  raw_hrefs <- unique(sub('^href="', "", sub('"$', "", raw_hrefs)))
  extra <- setdiff(raw_hrefs, href)
  href <- c(href, extra)
  text <- c(text, rep("", length(extra)))
  href <- gsub("&amp;", "&", href, fixed = TRUE)
  href <- gsub("&#58;", ":", href, fixed = TRUE)
  keep <- !is.na(href) & nzchar(href) & !grepl("^(javascript:|mailto:|#)", href)
  href <- href[keep]
  text <- text[keep]

  abs_url <- vapply(href, function(h) {
    if (grepl("^https?://", h)) return(h)
    if (grepl("^//", h)) return(paste0("https:", h))
    # a literal space in an href (maine.gov has them) makes url_absolute()
    # return NA; encode it first
    xml2::url_absolute(gsub(" ", "%20", h, fixed = TRUE), base)
  }, character(1), USE.NAMES = FALSE)
  abs_url <- gsub(" ", "%20", abs_url, fixed = TRUE)

  hit <- grepl(pattern, href, ignore.case = TRUE, perl = TRUE) |
    grepl(pattern, abs_url, ignore.case = TRUE, perl = TRUE)
  if (!is.null(exclude)) {
    hit <- hit & !grepl(exclude, abs_url, ignore.case = TRUE, perl = TRUE)
  }
  is_data <- grepl("\\.(xlsx|xlsm|xls|csv|tsv|pdf|zip|json)(\\?|$)", abs_url,
                   ignore.case = TRUE) |
    grepl("/download(\\?|$)", abs_url, ignore.case = TRUE)

  out <- data.frame(url = abs_url[hit], href = href[hit], text = text[hit],
                    stringsAsFactors = FALSE)
  out <- out[!duplicated(out$url), , drop = FALSE]
  rownames(out) <- NULL
  attr(out, "candidates") <- unique(abs_url[is_data & !hit])
  attr(out, "fetched") <- TRUE
  if (must_find && !nrow(out)) {
    stop("discover_links(", page_url, "): no link matched /", pattern, "/; ",
         length(attr(out, "candidates")), " other data links on the page",
         call. = FALSE)
  }
  out
}

# Fetch several files, politely spaced. `dest_fn` maps a URL to its dest
# path (or supply `dests` directly). Returns a list of records.
fetch_many <- function(urls, dest_fn = NULL, dests = NULL, polite = 3,
                       previous = NULL, ...) {
  if (is.null(dests)) dests <- vapply(urls, dest_fn, character(1), USE.NAMES = FALSE)
  stopifnot(length(urls) == length(dests))
  recs <- vector("list", length(urls))
  for (i in seq_along(urls)) {
    prev <- if (!is.null(previous)) previous[[dests[i]]] else NULL
    recs[[i]] <- fetch_file(urls[i], dests[i], previous = prev, ...)
    if (i < length(urls) && recs[[i]]$status %in% c("updated", "failed") && polite > 0) {
      Sys.sleep(polite)
    }
  }
  recs
}

# Socrata CSV export. `limit` guards against silent truncation: the API caps
# a query at `limit` rows, so a result that exactly fills it means the dataset
# grew past what we asked for.
socrata_csv <- function(domain, id, dest, limit = 50000L, query = NULL, ...) {
  url <- sprintf("https://%s/resource/%s.csv?$limit=%d", domain, id, as.integer(limit))
  if (!is.null(query)) url <- paste0(url, "&", query)
  not_full <- function(path) {
    n <- length(readLines(path, warn = FALSE)) - 1L
    if (n >= limit) {
      stop(sprintf("Socrata %s returned %d rows, which fills the $limit of %d; raise it",
                   id, n, limit))
    }
  }
  fetch_file(url, dest, type = "csv",
             headers = browser_headers(accept = "text/csv,*/*;q=0.8"),
             validate = not_full, conditional = FALSE, ...)
}

# ArcGIS REST layer, paged through /query and written to dest as CSV.
# `service_url` is the MapServer or FeatureServer root; `layer` its index.
arcgis_layer_csv <- function(service_url, layer = 0L, dest, where = "1=1",
                             out_fields = "*", page_size = 2000L,
                             headers = browser_headers(accept = "application/json"),
                             timeout = 120, ...) {
  base <- sprintf("%s/%d/query", sub("/+$", "", service_url), as.integer(layer))
  # Same contract as fetch_file(): a failure keeps the committed copy with a
  # warning, and stops only when there is nothing to fall back on.
  fail <- function(msg) {
    if (file.exists(dest)) {
      warning(sprintf("arcgis: %s could not be refreshed from %s (%s); keeping the committed copy",
                      basename(dest), base, msg), call. = FALSE)
      return(fetch_record(base, dest, "failed", bytes = file.size(dest),
                          sha256 = fetch_sha256(dest), attempts = 1L, error = msg))
    }
    stop(sprintf("arcgis: %s could not be downloaded from %s (%s) and there is no committed copy to fall back on",
                 basename(dest), base, msg), call. = FALSE)
  }
  rows <- list()
  offset <- 0L
  repeat {
    resp <- tryCatch(
      httr::GET(base, httr::add_headers(.headers = headers), httr::timeout(timeout),
                query = list(where = where, outFields = out_fields, f = "json",
                             returnGeometry = "false",
                             resultOffset = offset, resultRecordCount = page_size)),
      error = function(e) e
    )
    if (inherits(resp, "error")) return(fail(conditionMessage(resp)))
    if (httr::status_code(resp) != 200L) {
      return(fail(paste("HTTP", httr::status_code(resp))))
    }
    js <- tryCatch(jsonlite::fromJSON(httr::content(resp, "text", encoding = "UTF-8"),
                                      simplifyVector = TRUE),
                   error = function(e) NULL)
    if (is.null(js)) return(fail("response is not JSON"))
    if (!is.null(js$error)) return(fail(paste(js$error$message, collapse = " ")))
    feats <- js$features
    n <- if (is.null(feats)) 0L else nrow(feats)
    if (!n) break
    rows[[length(rows) + 1L]] <- feats$attributes
    if (!isTRUE(js$exceededTransferLimit) && n < page_size) break
    offset <- offset + n
  }
  if (!length(rows)) return(fail("no features returned"))
  out <- do.call(rbind, rows)
  tmp <- tempfile(fileext = ".csv")
  utils::write.csv(out, tmp, row.names = FALSE, na = "")
  problem <- fetch_validate(tmp, "csv", min_bytes = 64L)
  if (!is.null(problem)) { unlink(tmp); return(fail(problem)) }
  dir.create(dirname(dest), showWarnings = FALSE, recursive = TRUE)
  new_sha <- fetch_sha256(tmp)
  status <- if (file.exists(dest) && identical(new_sha, fetch_sha256(dest))) "unchanged" else "updated"
  if (status == "updated") file.copy(tmp, dest, overwrite = TRUE)
  unlink(tmp)
  fetch_record(base, dest, status, http_status = 200L, bytes = file.size(dest),
               sha256 = new_sha, attempts = 1L)
}

# CKAN package resources, as a data frame (name, url, format, last_modified),
# optionally filtered by a regex on name or url.
ckan_resources <- function(base, package_id, pattern = NULL,
                           headers = browser_headers(accept = "application/json"),
                           timeout = 120) {
  url <- sprintf("%s/api/3/action/package_show?id=%s", sub("/+$", "", base), package_id)
  resp <- httr::GET(url, httr::add_headers(.headers = headers), httr::timeout(timeout))
  if (httr::status_code(resp) != 200L) {
    stop("ckan_resources: HTTP ", httr::status_code(resp), " from ", url, call. = FALSE)
  }
  js <- jsonlite::fromJSON(httr::content(resp, "text", encoding = "UTF-8"),
                           simplifyVector = TRUE)
  if (!isTRUE(js$success)) stop("ckan_resources: package_show failed for ", package_id, call. = FALSE)
  res <- js$result$resources
  out <- data.frame(
    name = res$name %||% NA_character_,
    url = res$url,
    format = res$format %||% NA_character_,
    last_modified = res$last_modified %||% NA_character_,
    stringsAsFactors = FALSE
  )
  if (!is.null(pattern)) {
    out <- out[grepl(pattern, out$name, ignore.case = TRUE) |
                 grepl(pattern, out$url, ignore.case = TRUE), , drop = FALSE]
  }
  rownames(out) <- NULL
  out
}

# md5 of every file under raw/, excluding Excel lock files (~$...). The lock
# files depend on whether someone has a workbook open, which is not a change
# to the data; three states had them hashed into their change detection.
raw_state_md5 <- function(dir = "raw", pattern = NULL) {
  files <- list.files(dir, pattern = pattern, recursive = TRUE, full.names = TRUE)
  files <- files[!grepl("^~\\$", basename(files))]
  as.list(tools::md5sum(files))
}

# ---- fetch state in process.json ---------------------------------------------
#
# `process$fetch_state` is a named list keyed by dest path. record_fetch()
# merges records into it; commit_fetch_state() writes process.json only when
# something other than the timestamp changed, so a nightly run that finds
# nothing new does not churn the file (and the auto-commit that follows).

FETCH_STATE_KEYS <- c("url", "status", "http_status", "bytes", "sha256",
                      "etag", "last_modified", "attempts", "error")

record_fetch <- function(process, records) {
  if (is.null(process$fetch_state)) process$fetch_state <- list()
  if (!is.null(records$dest)) records <- list(records)
  for (r in records) {
    if (is.null(r) || is.null(r$dest)) next
    process$fetch_state[[r$dest]] <- r
  }
  process
}

fetch_state_signature <- function(state) {
  if (is.null(state) || !length(state)) return(character())
  keys <- sort(names(state))
  vapply(keys, function(k) {
    e <- state[[k]]
    paste(k, paste(vapply(FETCH_STATE_KEYS, function(f) {
      v <- e[[f]]
      if (fetch_has_value(v)) as.character(v) else ""
    }, character(1)), collapse = "|"), sep = "=")
  }, character(1))
}

# Write process.json if the fetch state differs from what is on disk. Safe to
# call whether or not the ingest already wrote process.json this run.
commit_fetch_state <- function(process, path = "process.json") {
  on_disk <- tryCatch(dcf::dcf_process_record(path), error = function(e) NULL)
  if (is.null(on_disk)) return(invisible(process))
  if (!identical(fetch_state_signature(on_disk$fetch_state),
                 fetch_state_signature(process$fetch_state))) {
    on_disk$fetch_state <- process$fetch_state
    dcf::dcf_process_record(path, updated = on_disk)
  }
  invisible(process)
}

# One-line summary of a set of records, for the dcf log.
fetch_summary <- function(records, label = "fetch") {
  if (!is.null(records$dest)) records <- list(records)
  st <- vapply(records, function(r) r$status, character(1))
  lv <- c("updated", "unchanged", "skipped", "failed")
  message(sprintf("%s: %d file(s): %s", label, length(st),
                  paste(sprintf("%d %s", tabulate(factor(st, levels = lv), nbins = 4L), lv),
                        collapse = ", ")))
  invisible(st)
}

# ---- sources.json ------------------------------------------------------------
#
# Each state directory carries sources.json, an array of source entries:
#   id, access, url, page_url, pattern, type, dest_glob, publish_month,
#   year_convention, latest_year_ingested, ci_reachable, notes,
#   last_check_time, last_check_ok, last_fetch_time, last_fetch_ok
# The ingest reads its URL and discovery pattern from here, so a source is
# declared once. It writes back only latest_year_ingested.

read_sources <- function(path = "sources.json") {
  if (!file.exists(path)) stop("no ", path, " in ", getwd(), call. = FALSE)
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

source_entry <- function(sources, id) {
  hit <- Filter(function(s) identical(s$id, id), sources)
  if (!length(hit)) stop("sources.json has no entry with id '", id, "'", call. = FALSE)
  hit[[1]]
}

write_sources <- function(sources, path = "sources.json") {
  jsonlite::write_json(sources, path, auto_unbox = TRUE, pretty = TRUE, null = "null", na = "null")
  invisible(sources)
}

# Record the latest school-year start ingested, on every entry (or on `ids`).
# Writes only when the value changes.
update_latest_year <- function(year, ids = NULL, path = "sources.json") {
  if (!file.exists(path)) return(invisible(NULL))
  year <- suppressWarnings(as.integer(year))
  if (!length(year) || is.na(year)) return(invisible(NULL))
  src <- read_sources(path)
  changed <- FALSE
  for (i in seq_along(src)) {
    if (!is.null(ids) && !(src[[i]]$id %in% ids)) next
    cur <- suppressWarnings(as.integer(src[[i]]$latest_year_ingested))
    if (is.null(src[[i]]$latest_year_ingested) || is.na(cur) || cur != year) {
      src[[i]]$latest_year_ingested <- year
      changed <- TRUE
    }
  }
  if (changed) write_sources(src, path)
  invisible(changed)
}

# Latest school-year start year in a standard frame's `time` column.
latest_school_year <- function(data) {
  if (!"time" %in% names(data)) return(NA_integer_)
  yrs <- suppressWarnings(as.integer(substr(as.character(data$time), 1, 4)))
  if (all(is.na(yrs))) NA_integer_ else max(yrs, na.rm = TRUE)
}
