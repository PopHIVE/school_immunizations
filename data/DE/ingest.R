library(dcf)
library(dplyr)
source("../../resources/rate_scale.R")
source("../../resources/school_year.R")
source("../../resources/fetch.R")
source("../../resources/pdf_table.R")

# =============================================================================
# DE - Kindergarten immunization status and coverage, statewide
# Source: Delaware DPH, annual school immunization survey.
#
# DPH publishes the survey as two one-page chart PDFs linked from
# https://dhss.delaware.gov/dph/dpc/school-immunizations/ (see sources.json):
#
#   School_Survey_Exemption_Rates.pdf  "Immunization Status of Surveyed
#     Kindergarteners": a 100% stacked bar per school year, 2016-17 onward,
#     with four labelled segments in x order: fully immunized, medical
#     exemption, religious exemption, out of compliance. The four sum to 100.
#   School_Survey_Coverage_Rates.pdf   "Kindergarten Immunization Coverage
#     Rates": one cluster of five bars per school year, in legend order DTaP,
#     MMR, Polio, Hep B, Varicella, with a horizontal "Target" line.
#
# The chart labels are text, so the values are read from word coordinates
# (pdf_words()) rather than by scraping the bars. There are no county figures:
# Delaware has three counties and DPH publishes statewide charts only, so the
# output is one statewide row per school year.
#
# Not every bar in the coverage chart carries a label. In 2016-17 and 2017-18
# only the Polio bar (the middle bar of the cluster) is labelled; the other
# four antigens for those years are NA with flag "missing". Later years have
# all five. A cluster with five labels is read in x order; a cluster with
# fewer is read by position, each label being assigned the bar slot nearest
# its centre, and a label that does not sit on a slot stops the run.
#
# Both PDFs are replaced in place at a fixed URL when DPH adds a year, so
# they are fetched with if_exists = "replace" and kept under fixed names in
# raw/. A failed fetch keeps the committed copy with a warning.
# =============================================================================

sources <- read_sources()
src <- source_entry(sources, "dph_school_survey")

process <- dcf::dcf_process_record()
prev <- process$fetch_state

CHARTS <- list(
  coverage = list(
    dest = "raw/School_Survey_Coverage_Rates.pdf",
    link = "_Coverage_Rates\\.pdf$",
    title = "Kindergarten Immunization Coverage Rates"
  ),
  exemption = list(
    dest = "raw/School_Survey_Exemption_Rates.pdf",
    link = "_Exemption_Rates\\.pdf$",
    title = "Immunization Status of Surveyed Kindergarteners"
  )
)

# The downloaded file has to be the chart it is named for: one page whose
# text carries the chart title. An HTML block page is already rejected by
# fetch_validate(); this catches a wrong or reorganised PDF.
check_chart <- function(role) {
  function(path) {
    if (pdftools::pdf_info(path)$pages != 1L) stop("expected a one-page chart")
    txt <- paste(pdftools::pdf_text(path), collapse = " ")
    if (!grepl(CHARTS[[role]]$title, txt, fixed = TRUE)) {
      stop("page does not carry the title '", CHARTS[[role]]$title, "'")
    }
  }
}

# ---- Download ----------------------------------------------------------------
# The index page being unreachable must not stop the run: raw/ is committed,
# so discovery warns and the parse proceeds on what is there. A chart whose
# link is missing is recorded as a failed fetch of the committed copy, so
# the state shows in process.json rather than staying at the last success.
links <- discover_links(src$page_url, src$pattern, must_find = FALSE)
recs <- list()
for (role in names(CHARTS)) {
  dest <- CHARTS[[role]]$dest
  hit <- links$url[grepl(CHARTS[[role]]$link, links$url, ignore.case = TRUE)]
  if (length(hit) != 1L) {
    msg <- if (isTRUE(attr(links, "fetched"))) {
      sprintf("expected one %s link on the index page, found %d", role, length(hit))
    } else {
      "index page could not be fetched"
    }
    if (isTRUE(attr(links, "fetched"))) {
      warning("DE: ", msg, "; keeping the committed copy of ", basename(dest),
              call. = FALSE)
    }
    if (file.exists(dest)) {
      recs[[role]] <- fetch_record(src$page_url, dest, "failed", bytes = file.size(dest),
                                   sha256 = fetch_sha256(dest), error = msg)
    }
    next
  }
  recs[[role]] <- fetch_file(hit, dest, type = "pdf", validate = check_chart(role),
                             if_exists = "replace", previous = prev[[dest]])
  Sys.sleep(3)
}
if (length(recs)) fetch_summary(recs, "DE charts")
process <- record_fetch(process, recs)

raw_state <- raw_state_md5()
script_hash <- as.character(tools::md5sum("ingest.R"))

if (!identical(process$raw_state, raw_state) ||
    !identical(process$script_hash, script_hash)) {

  for (ch in CHARTS) {
    if (!file.exists(ch$dest)) {
      stop("DE: ", ch$dest, " is missing and could not be downloaded.", call. = FALSE)
    }
  }

  YEAR_LABEL <- "^20[0-9]{2}-[0-9]{2}$"
  PCT_LABEL <- "^[0-9]{1,3}\\.[0-9]%$"

  # Words of a one-page chart with their vertical centre, after checking the
  # page is the chart expected.
  chart_words <- function(role) {
    path <- CHARTS[[role]]$dest
    check_chart(role)(path)
    w <- pdf_words(path, 1)
    w$ymid <- w$y + w$height / 2
    w
  }

  # ---- Exemption chart ---------------------------------------------------------
  # The school-year label sits at the left of its bar; the four segment labels
  # share its vertical centre. The axis ticks below the bars match no year.
  read_exemption_chart <- function() {
    w <- chart_words("exemption")
    years <- w[grepl(YEAR_LABEL, w$text), ]
    pcts <- w[grepl(PCT_LABEL, w$text), ]
    if (!nrow(years)) stop("DE: no school-year labels in the exemption chart", call. = FALSE)
    rows <- lapply(seq_len(nrow(years)), function(i) {
      hit <- pcts[abs(pcts$ymid - years$ymid[i]) <= years$height[i] / 2, ]
      hit <- hit[order(hit$x), ]
      if (nrow(hit) != 4L) {
        stop(sprintf("DE: exemption chart row %s has %d labels, expected 4",
                     years$text[i], nrow(hit)), call. = FALSE)
      }
      v <- pdf_number(hit$text)
      data.frame(
        label = years$text[i],
        pct_fully_immunized = v[1], pct_medical_exempt = v[2],
        pct_religious_exempt = v[3], pct_out_of_compliance = v[4],
        stringsAsFactors = FALSE
      )
    })
    out <- bind_rows(rows)
    total <- rowSums(out[, -1])
    if (any(abs(total - 100) > 0.5)) {
      stop("DE: exemption chart segments do not sum to 100 for ",
           paste(out$label[abs(total - 100) > 0.5], collapse = ", "), call. = FALSE)
    }
    out
  }

  # ---- Coverage chart ----------------------------------------------------------
  ANTIGENS <- c(DTaP = "pct_dtap", MMR = "pct_mmr", Polio = "pct_polio",
                "Hep B" = "pct_hep_b", Varicella = "pct_varicella")

  read_coverage_chart <- function() {
    w <- chart_words("coverage")
    years <- w[grepl(YEAR_LABEL, w$text), ]
    years <- years[order(years$x), ]
    if (nrow(years) < 2L) stop("DE: fewer than two school-year labels in the coverage chart", call. = FALSE)
    if (diff(range(years$y)) > 2) stop("DE: coverage chart year labels are not on one row", call. = FALSE)

    # The bar order within a cluster follows the legend, so the legend has
    # to read as expected before a rank is turned into an antigen.
    legend <- w[w$y > max(years$y) + 5, ]
    legend <- paste(legend$text[order(legend$x)], collapse = " ")
    expected <- paste(names(ANTIGENS), collapse = " ")
    if (!identical(sub(" Target$", "", legend), expected)) {
      stop("DE: coverage chart legend reads '", legend, "', expected '", expected, "'",
           call. = FALSE)
    }

    pitch <- median(diff(years$xmid))
    # Value labels are above the axis row and inside the plot; the y-axis
    # ticks at the left are excluded by x.
    pcts <- w[grepl(PCT_LABEL, w$text) & w$y < min(years$y) &
                w$xmid > years$xmid[1] - pitch / 2, ]
    pcts$cluster <- pdf_nearest_col(pcts$xmid, years$xmid)
    pcts$offset <- pcts$xmid - years$xmid[pcts$cluster]
    n_lab <- tabulate(pcts$cluster, nbins = nrow(years))
    if (any(n_lab > length(ANTIGENS))) {
      stop("DE: coverage chart cluster(s) with more than five labels: ",
           paste(years$text[n_lab > length(ANTIGENS)], collapse = ", "), call. = FALSE)
    }
    # Bar pitch from the full clusters: the spacing between neighbouring
    # labels within a cluster of five.
    full <- which(n_lab == length(ANTIGENS))
    if (!length(full)) stop("DE: no coverage chart cluster carries all five labels", call. = FALSE)
    bar_pitch <- median(unlist(lapply(full, function(k) {
      diff(sort(pcts$xmid[pcts$cluster == k]))
    })))

    rows <- lapply(seq_len(nrow(years)), function(k) {
      d <- pcts[pcts$cluster == k, ]
      d <- d[order(d$x), ]
      vals <- setNames(rep(NA_real_, length(ANTIGENS)), ANTIGENS)
      if (nrow(d) == length(ANTIGENS)) {
        vals[] <- pdf_number(d$text)
      } else if (nrow(d)) {
        slot <- round(d$offset / bar_pitch)
        off_slot <- abs(d$offset - slot * bar_pitch) > bar_pitch / 3
        if (any(off_slot) || any(abs(slot) > 2) || anyDuplicated(slot)) {
          stop(sprintf("DE: coverage chart %s has %d labels that do not sit on bar slots",
                       years$text[k], nrow(d)), call. = FALSE)
        }
        vals[slot + 3L] <- pdf_number(d$text)
      }
      data.frame(label = years$text[k], as.list(vals), stringsAsFactors = FALSE)
    })
    out <- bind_rows(rows)
    v <- unlist(out[, ANTIGENS])
    if (any(v < 80 | v > 100, na.rm = TRUE)) {
      stop("DE: coverage chart value outside 80-100", call. = FALSE)
    }
    partial <- years$text[n_lab < length(ANTIGENS)]
    if (length(partial)) {
      message("DE: coverage chart clusters with fewer than five labels: ",
              paste(sprintf("%s (%d)", partial, n_lab[n_lab < length(ANTIGENS)]), collapse = ", "))
    }
    for (col in ANTIGENS) {
      out[[sub("^pct_", "flag_", col)]] <- ifelse(is.na(out[[col]]), "missing", CENSOR_FLAG_NONE)
    }
    out
  }

  exemption <- read_exemption_chart()
  coverage <- read_coverage_chart()

  out <- full_join(exemption, coverage, by = "label") %>%
    mutate(
      time = as.Date(school_year_time_from_end(school_year_end_from_label(label))),
      geography = "10", geography_name = "Delaware",
      type = "state", grade = "Kindergarten"
    ) %>%
    select(time, geography, geography_name, type, grade,
           pct_fully_immunized, pct_medical_exempt, pct_religious_exempt,
           pct_out_of_compliance,
           pct_dtap, flag_dtap, pct_mmr, flag_mmr, pct_polio, flag_polio,
           pct_hep_b, flag_hep_b, pct_varicella, flag_varicella) %>%
    arrange(time)
  if (anyNA(out$time)) stop("DE: a chart label is not a school year", call. = FALSE)

  message(sprintf("DE: %d statewide rows, school years %s to %s",
                  nrow(out), min(out$time), max(out$time)))

  dir.create("standard", showWarnings = FALSE)
  out <- write_standard(out, "Delaware", "./standard/data.csv.gz", from = "percent")
  update_latest_year(latest_school_year(out))

  process$raw_state <- raw_state
  process$script_hash <- script_hash
  dcf::dcf_process_record(updated = process)
}
commit_fetch_state(process)
