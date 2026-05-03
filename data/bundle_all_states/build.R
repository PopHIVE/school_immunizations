# read data from data source projects
# and write to this project's `dist` directory

library(readr)
library(dplyr)
library(purrr)
library(tibble)

project_root <- normalizePath("../..", winslash = "/", mustWork = TRUE)
data_root <- file.path(project_root, "data")

state_files <- list.files(
  data_root,
  pattern = "^data\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)

state_files <- state_files[grepl("/standard/data\\.csv$", state_files)]
state_files <- state_files[!grepl("/bundle_all_states/", state_files)]
state_files <- sort(state_files)

read_one <- function(path) {
  first_line <- readLines(path, n = 1, warn = FALSE)
  delim <- if (grepl("\t", first_line)) "\t" else ","

  readr::read_delim(
    path,
    delim = delim,
    col_types = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE,
    progress = FALSE,
    na = c("", "NA")
  ) %>%
    mutate(source_file = gsub(paste0("^", project_root, "/?"), "", path))
}

data_list <- lapply(state_files, read_one)
all_cols <- unique(unlist(lapply(data_list, names)))

data_national <- bind_rows(lapply(data_list, function(df) {
  missing_cols <- setdiff(all_cols, names(df))
  if (length(missing_cols) > 0) {
    for (col in missing_cols) {
      df[[col]] <- NA
    }
  }
  df[, all_cols]
}))

dir.create("dist", showWarnings = FALSE)
readr::write_tsv(data_national, "dist/data.csv", na = "NA")
readr::write_tsv(data_national, gzfile("dist/data.csv.gz"), na = "NA")
