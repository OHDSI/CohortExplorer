# SETUP --------------------------------------------------------------------
library(magrittr)
# Pre-requisites ----
# remotes::install_github('OHDSI/CohortExplorer')

connectionDetails <- Eunomia::getEunomiaConnectionDetails()

cohortDefinitionSet <-
  CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "settings/CohortsToCreate.csv",
    jsonFolder = "cohorts",
    sqlFolder = "sql/sql_server",
    packageName = "SkeletonCohortDiagnosticsStudy",
    cohortFileNameValue = "cohortId"
  ) %>%  dplyr::tibble() |>
  dplyr::filter(cohortId == 17493)

cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohortEunomia")

# output folder information ----
outputFolder <-
  file.path("D:", "temp", "outputFolder", "eunomia")

## optionally delete previous execution ----
unlink(x = outputFolder,
       recursive = TRUE,
       force = TRUE)
dir.create(path = outputFolder,
           showWarnings = FALSE,
           recursive = TRUE)

# Execution ----
## Create cohort tables on remote ----
CohortGenerator::createCohortTables(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = 'main',
  cohortTableNames = cohortTableNames,
  incremental = TRUE
)
## Generate cohort on remote ----
CohortGenerator::generateCohortSet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = 'main',
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet,
  cohortDatabaseSchema = 'main',
  incremental = TRUE,
  incrementalFolder = file.path(outputFolder, "incremental")
)


# EXECUTE --------------------------------------------------------------------
tryCatch(
  expr = {
    CohortExplorer::createCohortExplorerApp(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = 'main',
      cdmDatabaseSchema = 'main',
      vocabularyDatabaseSchema = 'main',
      cohortTable = cohortTableNames$cohortTable,
      cohortDefinitionId = cohortDefinitionSet[1,]$cohortId,
      cohortName = cohortDefinitionSet[1,]$cohortName,
      exportFolder = file.path(outputFolder, "not_temp"),
      databaseId = 'eunomia',
      shiftDate = TRUE
    )
  },
  error = function(e) {
    
  }
)
