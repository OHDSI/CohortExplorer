# Code to test CohortExplorer on database platforms not available to GitHub Actions
library(CohortExplorer)

exportFolder <- "s:/temp/CohortExplorer"

# RedShift
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  connectionString = keyring::key_get("redShiftConnectionStringOhdaCcae"),
  user = keyring::key_get("redShiftUserName"),
  password = keyring::key_get("redShiftPassword")
)
cdmDatabaseSchema <- "cdm_truven_ccae_v2136"
cohortDatabaseSchema <- "results_truven_ccae_v2136"
cohortTable <- "cohort"
cohortDefinitionId <- 544
tempEmulationSchema <- NULL
databaseId <- "CCAE"

# BigQuery
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "bigquery",
  connectionString = keyring::key_get("bigQueryConnString"),
  user = "",
  password = "")

cdmDatabaseSchema <- "synpuf_2m"
cohortDatabaseSchema <- "synpuf_2m"
cohortTable <- "cohort"
cohortDefinitionId <- 787641790
tempEmulationSchema <- "synpuf_2m_results"
databaseId <- "Synpuf"

# Code to find cohorts in cohort table:
# library(dplyr)
# connection <- DatabaseConnector::connect(connectionDetails)
# cohort <- tbl(connection, DatabaseConnector::inDatabaseSchema(cohortDatabaseSchema, cohortTable))
# cohort %>%
#   group_by(cohort_definition_id) %>%
#   summarise(cohort_count = n()) %>%
#   collect() %>%
#   SqlRender::snakeCaseToCamelCaseNames()
# DatabaseConnector::disconnect(connection)

CohortExplorer::createCohortExplorerApp(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  vocabularyDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = tempEmulationSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = cohortDefinitionId,
  exportFolder = exportFolder,
  databaseId = databaseId,
  shiftDates = TRUE,
  assignNewId = TRUE
)

# Code to verify all temp tables have been cleaned up:
# connection <- DatabaseConnector::connect(connectionDetails)
# DatabaseConnector::dropEmulatedTempTables(connection, tempEmulationSchema)
# DatabaseConnector::disconnect(connection)
