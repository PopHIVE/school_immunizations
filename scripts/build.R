library(dcf)

#Create a repo for each state
#lapply(state.abb, function(X) dcf_add_source(X) )

dcf::dcf_build()

### dcf::dcf_add_bundle("bundle_all_states")


#dcf::dcf_check('AZ')
#dcf::dcf_process('AZ')

#Update mermaid diagram
#dcf_status_diagram()