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
  if (file.exists(file.path(
    "data",
    paste0("CohortExplorer_", cohortId, "_", databaseId, ".RData")
  ))) {
    return(readRDS(file = file.path(
      "data",
      paste0("CohortExplorer_", cohortId, "_", databaseId, ".RData")
    )))
  } else {
    return(NULL)
  }
}
