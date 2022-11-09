# library(shiny)
# library(ggplot2)
# library(DT)
# library(plotly)
# library(magrittr)
#
# source("configuration.R")
#
# cohortTable <- shinySettings$cohortTable
# cohortDatabaseSchema <- shinySettings$cohortDatabaseSchema
# cdmDatabaseSchema <- shinySettings$cdmDatabaseSchema
# cohortDefinitionId <- shinySettings$cohortDefinitionId
# vocabularyDatabaseSchema <- shinySettings$vocabularyDatabaseSchema
# tempEmulationSchema <- shinySettings$tempEmulationSchema
# sampleSize <- shinySettings$sampleSize
# originDate <- shinySettings$originDate
# conceptSetIds <- shinySettings$conceptSetIds
# conceptSets <- shinySettings$conceptSets
# cohortName <- shinySettings$cohortName
#
# connection <- DatabaseConnector::connect(connectionDetails)
# onStop(function() {
#   if (DBI::dbIsValid(connection)) {
#     DatabaseConnector::disconnect(connection = connection)
#   }
# })
#
# # take a random sample
# if (is.null(shinySettings$subjectIds)) {
#   sql <- "SELECT TOP @sample_size subject_id
#           FROM (
#           	SELECT DISTINCT subject_id
#           	FROM @cohort_database_schema.@cohort_table
#           	WHERE cohort_definition_id = @cohort_definition_id
#           	) all_ids
#           ORDER BY NEWID();"
#
#   writeLines("Attempting to find subjects in cohort table.")
#   shinySettings$subjectIds <-
#     DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = sql,
#       sample_size = sampleSize,
#       cohort_database_schema = cohortDatabaseSchema,
#       cohort_table = cohortTable,
#       cohort_definition_id = cohortDefinitionId
#     )[, 1]
# }
#
# subjectIds <- shinySettings$subjectIds
#
#
# if (length(subjectIds) == 0) {
#   stop("No subjects found in cohort ",
#        cohortDefinitionId)
# }
#
#
# source("extractPersonLevelData.R")
#
# subjects <- cohort %>%
#   dplyr::rename(personId = subjectId) %>%
#   dplyr::group_by(personId) %>%
#   dplyr::summarise(cohortStartDate = min(cohortStartDate)) %>%
#   dplyr::inner_join(person,
#                     by = "personId") %>%
#   dplyr::mutate(age = clock::get_year(cohortStartDate) - yearOfBirth) %>%
#   dplyr::inner_join(conceptIds,
#                     by = c("genderConceptId" = "conceptId")) %>%
#   dplyr::rename(gender = conceptName) %>%
#   dplyr::ungroup()
#
#
# tables <- c(
#   "conditionEra",
#   "conditionOccurrence",
#   "drugEra",
#   "drugExposure",
#   "procedureOccurrence",
#   "measurement",
#   "observation",
#   "visitOccurrence"
# )
#
# csvDownloadButton <- function(ns,
#                               outputTableId,
#                               buttonText = "Download CSV (filtered)") {
#   shiny::tagList(shiny::tags$br(),
#                  shiny::tags$button(
#                    buttonText,
#                    onclick = paste0("Reactable.downloadDataCSV('", outputTableId, "')")
#                  ))
# }
#
#
# DatabaseConnector::disconnect(connection = connection)
