test_that("Extract person level data", {
  skip_if(skipCdmTests, "cdm settings not configured")

  library(dplyr)

  createCohortTableSql <- "
    DROP TABLE IF EXISTS @cohort_database_schema.@cohort_table;

  CREATE TABLE @cohort_database_schema.@cohort_table (
  	cohort_definition_id BIGINT,
  	subject_id BIGINT,
  	cohort_start_date DATE,
  	cohort_end_date DATE
  );"

  DatabaseConnector::renderTranslateExecuteSql(
    connection = DatabaseConnector::connect(connectionDetails),
    sql = createCohortTableSql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable
  )

  outputDir <- tempfile()

  # database id has space
  testthat::expect_error(
    createCohortExplorerApp(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionId = c(1),
      sampleSize = 100,
      databaseId = "databaseData 001",
      exportFolder = outputDir
    )
  )

  # no connection or connectionDetails
  testthat::expect_error(
    createCohortExplorerApp(
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionId = c(1),
      sampleSize = 100,
      databaseId = "databaseData",
      exportFolder = outputDir
    )
  )

  # cohort table has no subjects
  testthat::expect_warning(
    createCohortExplorerApp(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionId = c(1),
      sampleSize = 100,
      databaseId = "databaseData",
      exportFolder = outputDir
    )
  )

  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  # create a cohort table using databaseData data
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = 1;
            INSERT INTO @cohort_database_schema.@cohort_table (cohort_definition_id,
                                                                subject_id,
                                                                cohort_start_date,
                                                                cohort_end_date)
            SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
            FROM
              (
                SELECT  1 cohort_definition_id,
                        10 subject_id,
                        CAST('20000101' AS DATE) cohort_start_date,
                        CAST('20101231' AS DATE) cohort_end_date
              ) a;
            ",
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable
  )

  createCohortExplorerApp(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortTable = cohortTable,
    cohortDefinitionId = c(1),
    sampleSize = 100,
    databaseId = "databaseData",
    exportFolder = outputDir
  )

  testthat::expect_true(file.exists(file.path(outputDir)))
  testthat::expect_true(file.exists(file.path(outputDir, "data")))

  createCohortExplorerApp(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortTable = cohortTable,
    cohortDefinitionId = c(1),
    sampleSize = 100,
    personIds = c(10, 11),
    databaseId = "databaseData",
    exportFolder = outputDir,
    assignNewId = TRUE,
    shiftDates = TRUE
  )

  outputPath <- createCohortExplorerApp(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortTable = cohortTable,
    cohortDefinitionId = c(1),
    sampleSize = 100,
    doNotExportCohortData = TRUE,
    databaseId = "databaseData",
    exportFolder = outputDir
  )

  testthat::expect_true(file.exists(
    file.path(
      outputDir,
      "data",
      "CohortExplorer_0_databaseData.rds"
    )
  ))
})
