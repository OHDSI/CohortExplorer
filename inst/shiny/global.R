readRDS(file = file.path("data", "CohortExplorer.rds"))

subjects <- cohort %>%
  dplyr::rename(personId = subjectId) %>%
  dplyr::group_by(personId) %>%
  dplyr::summarise(cohortStartDate = min(cohortStartDate)) %>%
  dplyr::inner_join(person,
                    by = "personId") %>%
  dplyr::mutate(age = clock::get_year(cohortStartDate) - yearOfBirth) %>%
  dplyr::inner_join(conceptIds,
                    by = c("genderConceptId" = "conceptId")) %>%
  dplyr::rename(gender = conceptName) %>%
  dplyr::ungroup()


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

csvDownloadButton <- function(ns,
                              outputTableId,
                              buttonText = "Download CSV (filtered)") {
  shiny::tagList(shiny::tags$br(),
                 shiny::tags$button(
                   buttonText,
                   onclick = paste0("Reactable.downloadDataCSV('", outputTableId, "')")
                 ))
}