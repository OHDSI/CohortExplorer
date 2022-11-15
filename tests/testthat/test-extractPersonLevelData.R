test_that("Extract person level data", {
  skip_if(skipCdmTests, "cdm settings not configured")

  library(dplyr)
  cohortTableNames <- "cohort"

  outputDir <- tempfile()

  # database id has space
  expect_error(
    exportPersonLevelData(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohortTable = "cohort",
      cohortDefinitionId = c(1),
      sampleSize = 100,
      databaseId = "databaseData 001",
      exportFolder = outputDir
    )
  )

  # no cohort table data, also checks if it can connect to data source
  expect_error(
    exportPersonLevelData(
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohortTable = "cohort",
      cohortDefinitionId = c(1),
      sampleSize = 100,
      databaseId = "databaseData",
      exportFolder = outputDir
    )
  )

  expect_warning(
    exportPersonLevelData(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohortTable = "cohort",
      cohortDefinitionId = c(1),
      sampleSize = 100,
      databaseId = "databaseData",
      exportFolder = outputDir
    )
  )

  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)

  # create a cohort table using databaseData data
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "DELETE FROM @cohort_database_schema.cohort;
            INSERT INTO @cohort_database_schema.cohort
            SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
            FROM
              (
                SELECT  1 cohort_definition_id,
                        10 subject_id, CAST('2000-01-01' AS DATE) cohort_start_date,
                        CAST('2010-12-31' AS DATE) cohort_end_date
              ) a;
            ",
    cohort_database_schema = cohortDatabaseSchema
  )

  exportPersonLevelData(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortTable = "cohort",
    cohortDefinitionId = c(1),
    sampleSize = 100,
    databaseId = "databaseData",
    exportFolder = outputDir
  )

  list.files(file.path(outputDir, "CohortExplorer"))

  testthat::expect_true(file.exists(file.path(outputDir, "CohortExplorer")))
  testthat::expect_true(file.exists(file.path(outputDir, "CohortExplorer", "data")))

  exportPersonLevelData(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortTable = "cohort",
    cohortDefinitionId = c(1),
    sampleSize = 100,
    personIds = c(1:100),
    databaseId = "databaseData",
    exportFolder = outputDir,
    assignNewId = TRUE,
    shiftDates = TRUE
  )
})
