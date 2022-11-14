test_that("Extract person level data", {
  skip_if(skipCdmTests, "cdm settings not configured")

  library(dplyr)
  cohortTableNames <- "cohort"

  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)

  # create a cohort table using eunomia data
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "DELETE FROM main.cohort;
            INSERT INTO main.cohort
            SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
            FROM
              (
                SELECT  1 cohort_definition_id,
                        10 subject_id, CAST('2000-01-01' AS DATE) cohort_start_date,
                        CAST('2010-12-31' AS DATE) cohort_end_date
              );
            "
  )

  outputDir <- tempfile()
  exportPersonLevelData(
    connection = connection,
    cohortDatabaseSchema = "main",
    cdmDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortDefinitionId = c(1),
    sampleSize = 100,
    databaseId = "eunomia",
    exportFolder = outputDir
  )

  list.files(file.path(outputDir, "CohortExplorer"))

  testthat::expect_true(file.exists(file.path(outputDir, "CohortExplorer")))
  testthat::expect_true(file.exists(file.path(outputDir, "CohortExplorer", "data")))
})
