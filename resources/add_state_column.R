add_state_column <- function(data, state_name) {
  if (!is.data.frame(data)) {
    stop("`data` must be a data frame.")
  }

  cols <- names(data)

  if ("state" %in% cols) {
    data$state <- state_name
    cols <- names(data)
  } else {
    data$state <- state_name
    cols <- c(cols, "state")
  }

  if (!"time" %in% cols) {
    return(data[, cols, drop = FALSE])
  }

  base_cols <- cols[cols != "state"]
  time_pos <- match("time", base_cols)
  new_cols <- append(base_cols, "state", after = time_pos)

  data[, new_cols, drop = FALSE]
}
