# SETUP --------------------------------------------------------------------

# Pre-requisites ----
remotes::install_github('OHDSI/CohortExplorer')

# connection details ----
# Details for connecting to the server:
# See ?DatabaseConnector::createConnectionDetails for help
connectionDetails <-
  DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = "some.server.com/ohdsi",
    user = "joe",
    password = "secret"
  )

# EXECUTE --------------------------------------------------------------------
CohortExplorer::exportPersonLevelData(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = "cohort",
  cdmDatabaseSchema = "cdm",
  vocabularyDatabaseSchema = "cdm",
  cohortTable = "cohort",
  cohortDefinitionId = 1234,
  exportFolder = "export",
  databaseId = "ccae",
  cohortName = "my cohort"
)
