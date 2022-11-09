csvDownloadButton <- function(ns,
                              outputTableId,
                              buttonText = "Download CSV (filtered)") {
  shiny::tagList(shiny::tags$br(),
                 shiny::tags$button(
                   buttonText,
                   onclick = paste0("Reactable.downloadDataCSV('", outputTableId, "')")
                 ))
}

readData <- function(databaseId,
                     cohortId) {
  return(readRDS(file = file.path(
    "data",
    paste0("CohortExplorer_", cohortId, "_", databaseId, ".RData")
  )))
}
