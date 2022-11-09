library(magrittr)
source("R/private.R")

listOfFiles <-
  dplyr::tibble(files = list.files(path = file.path("data"), pattern = ".RData"))

listOfFiles$newName <-
  gsub(
    pattern = "CohortExplorer_",
    replacement = "",
    fixed = TRUE,
    x = listOfFiles$files
  )
listOfFiles$newName <-
  gsub(
    pattern = ".RData",
    replacement = "",
    fixed = TRUE,
    x = listOfFiles$newName
  )

listOfFiles <- listOfFiles %>%
  tidyr::separate(
    col = newName,
    sep = "_",
    into = c("cohortId", "databaseId")
  ) %>%
  dplyr::arrange(cohortId, databaseId)


tables <- c(
  "conditionEra",
  "conditionOccurrence",
  "drugEra",
  "drugExposure",
  "procedureOccurrence",
  "measurement",
  "observation",
  "visitOccurrence"
)
