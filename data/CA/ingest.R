library(dcf)
library(tidyverse)

# check raw state
raw_state <- as.list(tools::md5sum(list.files(
  "raw", "csv", recursive = TRUE, full.names = TRUE
)))
process <- dcf::dcf_process_record()

# process raw if state has changed
if (!identical(process$raw_state, raw_state)) {
  
  
  ## CODE TO CLEAN RAW DATA 
  ##xxxx
  
  
  #Save standard file as a compressed csv
  vroom::vroom_write(data, './standard/data.csv.gz')
  
  # record processed raw state
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}