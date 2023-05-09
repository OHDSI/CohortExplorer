test_that("Create app with cohort data in temp table", {
  skip_if(skipCdmTests, "cdm settings not configured")
  
  library(dplyr)
  
  createCohortTableSql <- "
    DROP TABLE IF EXISTS #temp_cohort_table;

    SELECT o1.person_id subject_id,
            1 cohort_definition_id,
            o1.observation_period_start_date cohort_start_date,
            o1.observation_period_end_date cohort_end_date
    INTO #temp_cohort_table
    FROM @cdm_database_schema.observation_period o1
    INNER JOIN
          (
            SELECT person_id,
                    ROW_NUMBER() OVER (ORDER BY NEWID()) AS new_id
            FROM
              (
                SELECT DISTINCT person_id
                FROM @cdm_database_schema.observation_period
              ) a
          ) b
    ON o1.person_id = b.person_id
    WHERE new_id < 10
  ;"
  
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = createCohortTableSql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cdm_database_schema = cdmDatabaseSchema
  )
  
  outputDir <- tempfile()
  
  outputLocation <- createCohortExplorerApp(
    connection = connection,
    cohortDatabaseSchema = NULL,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortTable = "#temp_cohort_table",
    cohortDefinitionId = c(1),
    sampleSize = 100,
    databaseId = "databaseData",
    exportFolder = outputDir
  )
  
  testthat::expect_true(file.exists(file.path(outputDir, "data")))
  
  DatabaseConnector::disconnect(connection)
  
  unlink(x = outputDir,
         recursive = TRUE,
         force = TRUE)
})


test_that("Error because database has space", {
  skip_if(skipCdmTests, "cdm settings not configured")
  
  outputDir <- tempfile()
  
  testthat::expect_error(
    createCohortExplorerApp(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionId = c(1),
      sampleSize = 100,
      databaseId = "database Data",
      exportFolder = outputDir
    )
  )
  
  unlink(x = outputDir,
         recursive = TRUE,
         force = TRUE)
  
})

test_that("no connection or connection details", {
  skip_if(skipCdmTests, "cdm settings not configured")
  
  outputDir <- tempfile()
  
  testthat::expect_error(
    createCohortExplorerApp(
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionId = c(1),
      sampleSize = 100,
      databaseId = "database Data",
      exportFolder = outputDir
    )
  )
  
  unlink(x = outputDir,
         recursive = TRUE,
         force = TRUE)
})

test_that("Cohort has no data", {
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
  
  unlink(x = outputDir,
         recursive = TRUE,
         force = TRUE)
})


test_that("Create app with random sample of 100 subjects in cohort", {
  skip_if(skipCdmTests, "cdm settings not configured")
  
  library(dplyr)
  
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # create a cohort with 1000 persons
  createCohortTableSql <- "
    DROP TABLE IF EXISTS @cohort_database_schema.@cohort_table;

    SELECT o1.person_id subject_id,
            1 cohort_definition_id,
            o1.observation_period_start_date cohort_start_date,
            o1.observation_period_end_date cohort_end_date
    INTO @cohort_database_schema.@cohort_table
    FROM @cdm_database_schema.observation_period o1
    INNER JOIN
          (
            SELECT person_id,
                    ROW_NUMBER() OVER (ORDER BY NEWID()) AS new_id
            FROM
              (
                SELECT DISTINCT person_id
                FROM @cdm_database_schema.observation_period
              ) a
          ) b
    ON o1.person_id = b.person_id
    WHERE new_id <= 1000
  ;"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = createCohortTableSql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_table = cohortTable
  )
  
  outputDir <- tempfile()
  
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
  
  DatabaseConnector::disconnect(connection)
  
  unlink(x = outputDir,
         recursive = TRUE,
         force = TRUE)
  
})



test_that("Create app with random sample of 100 subjects in cohort with date shifting", {
  skip_if(skipCdmTests, "cdm settings not configured")
  
  library(dplyr)
  
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # create a cohort with 1000 persons
  createCohortTableSql <- "
    DROP TABLE IF EXISTS @cohort_database_schema.@cohort_table;

    SELECT o1.person_id subject_id,
            1 cohort_definition_id,
            o1.observation_period_start_date cohort_start_date,
            o1.observation_period_end_date cohort_end_date
    INTO @cohort_database_schema.@cohort_table
    FROM @cdm_database_schema.observation_period o1
    INNER JOIN
          (
            SELECT person_id,
                    ROW_NUMBER() OVER (ORDER BY NEWID()) AS new_id
            FROM
              (
                SELECT DISTINCT person_id
                FROM @cdm_database_schema.observation_period
              ) a
          ) b
    ON o1.person_id = b.person_id
    WHERE new_id <= 1000
  ;"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = createCohortTableSql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_table = cohortTable
  )
  
  outputDir <- tempfile()
  
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
  
  testthat::expect_true(file.exists(file.path(outputDir)))
  testthat::expect_true(file.exists(file.path(outputDir, "data")))
  
  DatabaseConnector::disconnect(connection)
  
  unlink(x = outputDir,
         recursive = TRUE,
         force = TRUE)
  
})

  
test_that("do Not Export CohortData", {
  skip_if(skipCdmTests, "cdm settings not configured")
  
  library(dplyr)
  
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  createCohortTableSql <- "
    DROP TABLE IF EXISTS @cohort_database_schema.@cohort_table;"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = createCohortTableSql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable
  )
  
  outputPath <- createCohortExplorerApp(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortTable = cohortTable,
    sampleSize = 100,
    doNotExportCohortData = TRUE,
    databaseId = "databaseData",
    exportFolder = outputDir
  )
  
  testthat::expect_true(file.exists(
    file.path(outputDir,
              "data",
              "CohortExplorer_0_databaseData.rds")
  ))
  
  DatabaseConnector::disconnect(connection)
  
  unlink(x = outputDir,
         recursive = TRUE,
         force = TRUE)
})
