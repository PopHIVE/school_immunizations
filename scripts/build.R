library(dcf)

#Create a repo for each state
lapply(state.abb, function(X) dcf_add_source(x) )
