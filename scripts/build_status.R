# Summarise the last build and fail loudly when something went wrong.
#
# dcf::dcf_build() catches each ingest's error, records it in that state's
# process.json, and carries on, so a broken parser or a failed download has
# never failed CI. This script reads every data/<ST>/process.json after a
# build, writes data/BUILD_STATUS.md, and exits non-zero if any ingest ended
# with success = false, or any fetch recorded in fetch_state ended "failed"
# with no file on disk to fall back on.
#
# Usage, from the repo root:  Rscript scripts/build_status.R [--out=path]

args <- commandArgs(trailingOnly = TRUE)
out_md <- sub("^--out=", "", grep("^--out=", args, value = TRUE))
if (!length(out_md)) out_md <- "data/BUILD_STATUS.md"

state_dirs <- list.dirs("data", recursive = FALSE, full.names = FALSE)
state_dirs <- state_dirs[grepl("^[A-Z]{2}$", state_dirs)]

count_rows <- function(path) {
  if (!file.exists(path)) return(NA_integer_)
  con <- gzfile(path, "r")
  on.exit(close(con))
  n <- 0L
  repeat {
    chunk <- readLines(con, n = 10000L, warn = FALSE)
    if (!length(chunk)) break
    n <- n + length(chunk)
  }
  max(n - 1L, 0L)
}

latest_year <- function(path) {
  if (!file.exists(path)) return(NA_integer_)
  con <- gzfile(path, "r")
  on.exit(close(con))
  hdr <- strsplit(readLines(con, n = 1L, warn = FALSE), ",", fixed = TRUE)[[1]]
  i <- match("time", gsub('"', "", hdr))
  if (is.na(i)) return(NA_integer_)
  yrs <- integer()
  repeat {
    chunk <- readLines(con, n = 20000L, warn = FALSE)
    if (!length(chunk)) break
    v <- vapply(strsplit(chunk, ",", fixed = TRUE), function(x) if (length(x) >= i) x[i] else "", "")
    yrs <- c(yrs, suppressWarnings(as.integer(substr(gsub('"', "", v), 1, 4))))
  }
  if (all(is.na(yrs))) NA_integer_ else max(yrs, na.rm = TRUE)
}

rows <- list()
failed <- character()
for (st in state_dirs) {
  dir <- file.path("data", st)
  p <- tryCatch(jsonlite::read_json(file.path(dir, "process.json")), error = function(e) NULL)
  script <- if (!is.null(p) && length(p$scripts)) p$scripts[[1]] else list()
  ok <- isTRUE(script$last_status$success)
  last_run <- script$last_run %||% ""
  log <- script$last_status$log
  err <- if (!ok && length(log)) tail(unlist(log), 1) else ""

  fs <- p$fetch_state
  fetch_txt <- ""
  fetch_bad <- FALSE
  if (length(fs)) {
    st_tab <- table(vapply(fs, function(r) r$status %||% "?", ""))
    fetch_txt <- paste(sprintf("%d %s", as.integer(st_tab), names(st_tab)), collapse = ", ")
    for (d in names(fs)) {
      if (identical(fs[[d]]$status, "failed") && !file.exists(file.path(dir, d))) fetch_bad <- TRUE
    }
  }

  data_path <- file.path(dir, "standard", "data.csv.gz")
  has_ingest <- file.exists(file.path(dir, "ingest.R")) &&
    length(grep("^\\s*[^#[:space:]]", readLines(file.path(dir, "ingest.R"), warn = FALSE))) > 0
  n <- count_rows(data_path)
  yr <- latest_year(data_path)

  state_ok <- ok && !fetch_bad
  if (!state_ok) failed <- c(failed, st)
  rows[[length(rows) + 1L]] <- data.frame(
    state = st,
    ingest = if (!has_ingest) "stub" else if (ok) "ok" else "FAILED",
    last_run = last_run,
    fetches = if (fetch_bad) paste(fetch_txt, "(no fallback file)") else fetch_txt,
    rows = ifelse(is.na(n), "", format(n, big.mark = ",")),
    latest_year = ifelse(is.na(yr), "", as.character(yr)),
    error = gsub("|", "\\|", substr(err, 1, 160), fixed = TRUE),
    stringsAsFactors = FALSE
  )
}
`%||%` <- function(a, b) if (is.null(a)) b else a

df <- do.call(rbind, rows)
lines <- c(
  "# Build status",
  "",
  sprintf("Written %s UTC by `scripts/build_status.R` after `dcf::dcf_build()`. Do not edit by hand.",
          format(Sys.time(), "%Y-%m-%d %H:%M", tz = "UTC")),
  "",
  "| State | Ingest | Last run | Fetches | Rows | Latest year | Error |",
  "|---|---|---|---|---|---|---|",
  sprintf("| %s | %s | %s | %s | %s | %s | %s |", df$state, df$ingest, df$last_run,
          df$fetches, df$rows, df$latest_year, df$error)
)
writeLines(lines, out_md)

message(sprintf("build_status: %d state(s), %d failed: %s", nrow(df), length(failed),
                paste(failed, collapse = " ")))
if (length(failed)) quit(status = 1L)
quit(status = 0L)
