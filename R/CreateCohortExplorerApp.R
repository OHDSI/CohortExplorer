# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of CohortExplorer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Create Cohort explorer shiny app with person level data
#'
#' @description
#' Export person level data from omop cdm tables for eligible persons in the cohort. Creates a folder with files
#' that are part of the Cohort Explorer Shiny app. This app may then be run to review person level profiles.
#'
#' @template Connection
#'
#' @template CohortDatabaseSchema
#'
#' @template CdmDatabaseSchema
#'
#' @template VocabularyDatabaseSchema
#'
#' @template CohortTable
#'
#' @template TempEmulationSchema
#'
#' @param cohortDefinitionId          The cohort id to extract records.
#'
#' @param cohortName                  (optional) Cohort Name
#'
#' @param sampleSize                  (Optional, default = 20) The number of persons to randomly sample. Ignored, if personId is given.
#'
#' @param personIds                   (Optional) An array of personId's to look for in Cohort table and CDM.
#'
#' @param exportFolder                The folder where the output will be exported to. If this folder
#'                                    does not exist it will be created.
#' @param databaseId                  A short string for identifying the database (e.g. 'Synpuf'). This will be displayed
#'                                    in shiny app to toggle between databases. Should not have space or underscore (_).
#'
#' @param shiftDates                  (Default = FALSE) Do you want to shift dates? This will help further de-identify data. The shift
#'                                    is the process of recalibrating dates such that all persons min(observation_period_start_date) is
#'                                    2000-01-01.
#'
#' @param assignNewId                 (Default = FALSE) Do you want to assign a newId for persons. This will replace the personId in the source with a randomly assigned newId.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "ohdsi.com",
#'   port = 5432,
#'   user = "me",
#'   password = "secure"
#' )
#'
#' createCohortExplorerApp(
#'   connectionDetails = connectionDetails,
#'   cohortDefinitionId = 1234
#' )
#' }
#'
#' @export
createCohortExplorerApp <- function(connectionDetails = NULL,
                                    connection = NULL,
                                    cohortDatabaseSchema = "cohort",
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema = cdmDatabaseSchema,
                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                    cohortTable = "cohort",
                                    cohortDefinitionId,
                                    cohortName = NULL,
                                    sampleSize = 25,
                                    personIds = NULL,
                                    exportFolder,
                                    databaseId,
                                    shiftDates = FALSE,
                                    assignNewId = FALSE) {
  startTime <- Sys.time()

  errorMessage <- checkmate::makeAssertCollection()

  checkmate::assertCharacter(
    x = cohortDatabaseSchema,
    min.len = 1,
    add = errorMessage
  )

  checkmate::assertCharacter(
    x = cdmDatabaseSchema,
    min.len = 1,
    add = errorMessage
  )

  checkmate::assertCharacter(
    x = vocabularyDatabaseSchema,
    min.len = 1,
    add = errorMessage
  )

  checkmate::assertCharacter(
    x = cohortTable,
    min.len = 1,
    add = errorMessage
  )

  checkmate::assertCharacter(
    x = databaseId,
    min.len = 1,
    max.len = 1,
    add = errorMessage
  )

  checkmate::assertCharacter(
    x = tempEmulationSchema,
    min.len = 1,
    null.ok = TRUE,
    add = errorMessage
  )

  checkmate::assertIntegerish(
    x = cohortDefinitionId,
    lower = 0,
    len = 1,
    add = errorMessage
  )

  checkmate::assertIntegerish(
    x = sampleSize,
    lower = 0,
    len = 1,
    null.ok = TRUE,
    add = errorMessage
  )

  if (is.null(personIds)) {
    checkmate::assertIntegerish(
      x = sampleSize,
      lower = 0,
      len = 1,
      null.ok = TRUE,
      add = errorMessage
    )
  }

  checkmate::assertIntegerish(
    x = personIds,
    lower = 0,
    min.len = 1,
    null.ok = TRUE,
    add = errorMessage
  )

  exportFolder <- normalizePath(exportFolder, mustWork = FALSE)

  dir.create(
    path = exportFolder,
    showWarnings = FALSE,
    recursive = TRUE
  )

  checkmate::assertDirectory(
    x = exportFolder,
    access = "x",
    add = errorMessage
  )

  checkmate::reportAssertions(collection = errorMessage)

  originalDatabaseId <- databaseId

  databaseId <- as.character(gsub(
    pattern = " ",
    replacement = "",
    x = databaseId
  ))

  databaseId <- as.character(gsub(
    pattern = "_",
    replacement = "",
    x = databaseId
  ))

  if (nchar(databaseId) < nchar(originalDatabaseId)) {
    stop(paste0(
      "databaseId should not have space or underscore: ",
      originalDatabaseId
    ))
  }

  rdsFileName <- paste0(
    "CohortExplorer_",
    cohortDefinitionId,
    "_",
    databaseId,
    ".RData"
  )

  # Set up connection to server ----------------------------------------------------
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }

  if (!is.null(personIds)) {
    persons <- dplyr::tibble(personId = personIds) %>%
      dplyr::mutate(randomNumber = runif(n = 1)) %>%
      dplyr::arrange(.data$randomNumber) %>%
      dplyr::mutate(newId = dplyr::row_number()) %>%
      dplyr::select(-.data$randomNumber)

    DatabaseConnector::insertTable(
      connection = connection,
      tableName = "#persons_filter",
      createTable = TRUE,
      dropTableIfExists = TRUE,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = TRUE,
      bulkLoad = (Sys.getenv("bulkLoad") == TRUE),
      camelCaseToSnakeCase = TRUE,
      data = persons
    )
    sampleSize <- nrow(persons)
  } else {
    # take a random sample
    sql <- "DROP TABLE IF EXISTS #persons_filter;
              SELECT *
              INTO #persons_filter
              FROM
              (
                SELECT ROW_NUMBER() OVER (ORDER BY NEWID()) AS new_id, person_id
                FROM (
                    	SELECT DISTINCT subject_id person_id
                    	FROM @cohort_database_schema.@cohort_table
                    	WHERE cohort_definition_id = @cohort_definition_id
                	) all_ids
              ) f
              WHERE new_id <= @sample_size;"

    writeLines("Attempting to find subjects in cohort table.")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      sample_size = sampleSize,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      cohort_definition_id = cohortDefinitionId
    )
  }

  writeLines("Getting cohort table.")
  cohort <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT c.subject_id,
                      p.new_id,
              	cohort_start_date AS start_date,
              	cohort_end_date AS end_date
              FROM @cohort_database_schema.@cohort_table c
              INNER JOIN #persons_filter p
              ON c.subject_id = p.person_id
              WHERE cohort_definition_id = @cohort_definition_id
          ORDER BY c.subject_id, cohort_start_date;",
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    tempEmulationSchema = tempEmulationSchema,
    cohort_definition_id = cohortDefinitionId,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble()

  if (nrow(cohort) == 0) {
    stop("Cohort does not have the selected subject ids. No shiny app created.")
  }

  writeLines("Getting person table.")
  person <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT p.person_id,
                    pf.new_id,
                gender_concept_id,
                year_of_birth
        FROM @cdm_database_schema.person p
        INNER JOIN #persons_filter pf
        ON p.person_id = pf.person_id
        ORDER BY p.person_id;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble()

  person <- person %>%
    dplyr::inner_join(
      cohort %>%
        dplyr::group_by(.data$subjectId) %>%
        dplyr::summarise(
          yearOfCohort = min(clock::get_year(.data$startDate)),
          .groups = "keep"
        ) %>%
        dplyr::ungroup() %>%
        dplyr::rename("personId" = .data$subjectId),
      by = "personId"
    ) %>%
    dplyr::mutate(age = .data$yearOfCohort - .data$yearOfBirth) %>%
    dplyr::select(-.data$yearOfCohort, -.data$yearOfBirth)

  writeLines("Getting observation period table.")
  observationPeriod <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT op.person_id,
                    p.new_id,
                    observation_period_start_date AS start_date,
                    observation_period_end_date AS end_date,
                    period_type_concept_id AS type_concept_id
              FROM @cdm_database_schema.observation_period op
              INNER JOIN #persons_filter p
              ON op.person_id = p.person_id
              ORDER BY op.person_id,
                      p.new_id,
                      observation_period_start_date,
                      observation_period_end_date;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble()

  writeLines("Getting visit occurrence table.")
  visitOccurrence <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT v.person_id,
              p.new_id,
              visit_start_date AS start_date,
              visit_end_date AS end_date,
              visit_concept_id AS concept_id,
          	  visit_type_concept_id AS type_concept_id,
          	  visit_source_concept_id AS source_concept_id,
          	  count(*) records
        FROM @cdm_database_schema.visit_occurrence v
        INNER JOIN #persons_filter p
        ON v.person_id = p.person_id
        GROUP BY v.person_id,
                  p.new_id,
                  visit_start_date,
                  visit_end_date,
                  visit_concept_id,
                  visit_type_concept_id,
                  visit_source_concept_id
        ORDER BY v.person_id,
                p.new_id,
                visit_start_date,
                visit_end_date,
                visit_concept_id,
                visit_type_concept_id,
                visit_source_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble()

  writeLines("Getting condition occurrence table.")
  conditionOccurrence <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT c.person_id,
              p.new_id,
              condition_start_date AS start_date,
              condition_end_date AS end_date,
              condition_concept_id AS concept_id,
          	  condition_type_concept_id AS type_concept_id,
          	  condition_source_concept_id AS source_concept_id,
          	  count(*) records
        FROM @cdm_database_schema.condition_occurrence c
        INNER JOIN #persons_filter p
        ON c.person_id = p.person_id
        GROUP BY c.person_id,
                  p.new_id,
                  condition_start_date,
                  condition_end_date,
                  condition_concept_id,
                  condition_type_concept_id,
                  condition_source_concept_id
        ORDER BY c.person_id,
                  p.new_id,
                  condition_start_date,
                  condition_end_date,
                  condition_concept_id,
                  condition_type_concept_id,
                  condition_source_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble()

  writeLines("Getting condition era table.")
  conditionEra <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT ce.person_id,
              p.new_id,
              condition_era_start_date AS start_date,
              condition_era_end_date AS end_date,
              condition_concept_id AS concept_id,
          	  count(*) records
        FROM @cdm_database_schema.condition_era ce
        INNER JOIN #persons_filter p
        ON ce.person_id = p.person_id
        GROUP BY ce.person_id,
              p.new_id,
              condition_era_start_date,
              condition_era_end_date,
              condition_concept_id
        ORDER BY ce.person_id,
              p.new_id,
              condition_era_start_date,
              condition_era_end_date,
              condition_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble() %>%
    dplyr::mutate(typeConceptId = 0, records = 1)

  writeLines("Getting observation table.")
  observation <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT o.person_id,
              p.new_id,
              observation_date AS start_date,
              observation_concept_id AS concept_id,
          	  observation_type_concept_id AS type_concept_id,
          	  observation_source_concept_id AS source_concept_id,
          	  count(*) records
        FROM @cdm_database_schema.observation o
        INNER JOIN #persons_filter p
        ON o.person_id = p.person_id
        GROUP BY o.person_id,
                  p.new_id,
                  observation_date,
                  observation_concept_id,
                  observation_type_concept_id,
                  observation_source_concept_id
        ORDER BY o.person_id,
                p.new_id,
                observation_date,
                observation_concept_id,
                observation_type_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble()

  writeLines("Getting procedure occurrence table.")
  procedureOccurrence <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT p.person_id,
              pf.new_id,
              procedure_date AS start_date,
              procedure_concept_id AS concept_id,
              procedure_type_concept_id AS type_concept_id,
              procedure_source_concept_id AS source_concept_id,
          	  count(*) records
        FROM @cdm_database_schema.procedure_occurrence p
        INNER JOIN #persons_filter pf
        ON p.person_id = pf.person_id
        GROUP BY p.person_id,
                  pf.new_id,
                  procedure_date,
                  procedure_concept_id,
                  procedure_type_concept_id,
                  procedure_source_concept_id
        ORDER BY p.person_id,
                pf.new_id,
                procedure_date,
                procedure_concept_id,
                procedure_type_concept_id,
                procedure_source_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble() %>%
    dplyr::mutate(endDate = .data$startDate)

  writeLines("Getting drug exposure table.")
  drugExposure <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT de.person_id,
              pf.new_id,
              drug_exposure_start_date AS start_date,
              drug_exposure_end_date AS end_date,
              drug_concept_id AS concept_id,
              drug_type_concept_id AS type_concept_id,
              drug_source_concept_id AS source_concept_id,
          	  count(*) records
        FROM @cdm_database_schema.drug_exposure de
        INNER JOIN #persons_filter pf
        ON de.person_id = pf.person_id
        GROUP BY de.person_id,
                  pf.new_id,
                  drug_exposure_start_date,
                  drug_exposure_end_date,
                  drug_concept_id,
                  drug_type_concept_id,
                  drug_source_concept_id
        ORDER BY de.person_id,
                  pf.new_id,
                  drug_exposure_start_date,
                  drug_exposure_end_date,
                  drug_concept_id,
                  drug_type_concept_id,
                  drug_source_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble()

  writeLines("Getting drug era table.")
  drugEra <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT de.person_id,
              pf.new_id,
              drug_era_start_date AS start_date,
              drug_era_end_date AS end_date,
              drug_concept_id AS concept_id,
          	  count(*) records
        FROM @cdm_database_schema.drug_era de
        INNER JOIN #persons_filter pf
        ON de.person_id = pf.person_id
        GROUP BY de.person_id,
                  pf.new_id,
                  drug_era_start_date,
                  drug_era_end_date,
                  drug_concept_id
        ORDER BY de.person_id,
                  pf.new_id,
                  drug_era_start_date,
                  drug_era_end_date,
                  drug_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble() %>%
    dplyr::mutate(typeConceptId = 0)

  writeLines("Getting measurement table.")
  measurement <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT m.person_id,
              pf.new_id,
              measurement_date AS start_date,
              measurement_concept_id AS concept_id,
              measurement_type_concept_id as type_concept_id,
              measurement_source_concept_id as source_concept_id,
          	  count(*) records
        FROM @cdm_database_schema.measurement m
        INNER JOIN #persons_filter pf
        ON m.person_id = pf.person_id
        GROUP BY m.person_id,
                  pf.new_id,
                  measurement_date,
                  measurement_concept_id,
                  measurement_type_concept_id,
                  measurement_source_concept_id
        ORDER BY m.person_id,
                  pf.new_id,
                  measurement_date,
                  measurement_concept_id,
                  measurement_type_concept_id,
                  measurement_source_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble() %>%
    dplyr::mutate(endDate = .data$startDate)


  writeLines("Getting concept id.")
  conceptIds <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "WITH concepts as
        (
          SELECT DISTINCT gender_concept_id AS CONCEPT_ID
          FROM @cdm_database_schema.person p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT period_type_concept_id AS CONCEPT_ID
          FROM @cdm_database_schema.observation_period p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT observation_concept_id AS CONCEPT_ID
          FROM @cdm_database_schema.observation p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT observation_type_concept_id AS CONCEPT_ID
          FROM @cdm_database_schema.observation p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT observation_source_concept_id AS CONCEPT_ID
          FROM @cdm_database_schema.observation p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT drug_concept_id AS concept_id
          FROM @cdm_database_schema.drug_exposure p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT drug_type_concept_id AS concept_id
          FROM @cdm_database_schema.drug_exposure p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT drug_source_concept_id AS concept_id
          FROM @cdm_database_schema.drug_exposure p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT drug_concept_id AS concept_id
          FROM @cdm_database_schema.drug_era p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT visit_concept_id AS concept_id
          FROM @cdm_database_schema.visit_occurrence p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT visit_type_concept_id AS concept_id
          FROM @cdm_database_schema.visit_occurrence p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT visit_source_concept_id AS concept_id
          FROM @cdm_database_schema.visit_occurrence p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT procedure_concept_id AS concept_id
          FROM @cdm_database_schema.procedure_occurrence p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT procedure_type_concept_id AS concept_id
          FROM @cdm_database_schema.procedure_occurrence p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT procedure_source_concept_id AS concept_id
          FROM @cdm_database_schema.procedure_occurrence p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT condition_concept_id AS concept_id
          FROM @cdm_database_schema.condition_occurrence p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT condition_type_concept_id AS concept_id
          FROM @cdm_database_schema.condition_occurrence p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT condition_source_concept_id AS concept_id
          FROM @cdm_database_schema.condition_occurrence p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT condition_concept_id AS concept_id
          FROM @cdm_database_schema.condition_era p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT measurement_concept_id AS concept_id
          FROM @cdm_database_schema.measurement p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT measurement_type_concept_id AS concept_id
          FROM @cdm_database_schema.measurement p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id

          UNION

          SELECT DISTINCT measurement_source_concept_id AS concept_id
          FROM @cdm_database_schema.measurement p
          INNER JOIN #persons_filter pf
          ON p.person_id = pf.person_id
        )
        SELECT DISTINCT c.concept_id,
                c.domain_id,
                c.concept_name,
                c.vocabulary_id,
                c.concept_code
        FROM @vocabulary_database_schema.concept c
        INNER JOIN
            concepts c2
        ON c.concept_id = c2.concept_id
        ORDER BY c.concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    vocabulary_database_schema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble()

  cohort <- cohort %>%
    dplyr::rename(personId = .data$subjectId)

  subjects <- cohort %>%
    dplyr::group_by(.data$personId) %>%
    dplyr::summarise(startDate = min(.data$startDate)) %>%
    dplyr::inner_join(person,
      by = "personId"
    ) %>%
    dplyr::inner_join(conceptIds,
      by = c("genderConceptId" = "conceptId")
    ) %>%
    dplyr::rename(gender = .data$conceptName) %>%
    dplyr::ungroup()

  personMinObservationPeriodDate <- observationPeriod %>%
    dplyr::group_by(.data$personId) %>%
    dplyr::summarise(
      minObservationPeriodDate = min(.data$startDate),
      .groups = "keep"
    ) %>%
    dplyr::ungroup()

  shiftDatesInData <- function(data,
                               originDate = as.Date("2000-01-01"),
                               minObservationPeriodDate = personMinObservationPeriodDate) {
    data <- data %>%
      dplyr::inner_join(personMinObservationPeriodDate,
        by = "personId"
      )

    if ("startDate" %in% colnames(data)) {
      data <- data %>%
        dplyr::mutate(startDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = .data$startDate,
              time2 = .data$minObservationPeriodDate,
              units = "days"
            )
          )
        ))
    }

    if ("endDate" %in% colnames(data)) {
      data <- data %>%
        dplyr::mutate(endDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = .data$endDate,
              time2 = .data$minObservationPeriodDate,
              units = "days"
            )
          )
        ))
    }

    data <- data %>%
      dplyr::select(-minObservationPeriodDate)
  }

  if (shiftDates) {
    observationPeriod <- shiftDatesInData(data = observationPeriod)
    cohort <- shiftDatesInData(data = cohort)
    conditionEra <- shiftDatesInData(data = conditionEra)
    conditionOccurrence <- shiftDatesInData(
      data =
        conditionOccurrence
    )
    drugExposure <- shiftDatesInData(data = drugExposure)
    measurement <- shiftDatesInData(data = measurement)
    observation <- shiftDatesInData(data = observation)
    procedureOccurrence <- shiftDatesInData(
      data =
        procedureOccurrence
    )
    visitOccurrence <- shiftDatesInData(data = visitOccurrence)
    measurement <- shiftDatesInData(data = measurement)
  }

  replaceId <- function(data, useNewId = TRUE) {
    if (useNewId) {
      data <- data %>%
        dplyr::select(-.data$personId) %>%
        dplyr::rename("personId" = .data$newId)
    } else {
      data <- data %>%
        dplyr::select(-.data$newId)
    }
    return(data)
  }

  cohort <- replaceId(data = cohort, useNewId = assignNewId)
  person <- replaceId(data = person, useNewId = assignNewId)
  subjects <- replaceId(data = subjects, useNewId = assignNewId)
  observationPeriod <- replaceId(
    data = observationPeriod,
    useNewId = assignNewId
  )
  visitOccurrence <- replaceId(
    data = visitOccurrence,
    useNewId = assignNewId
  )
  conditionOccurrence <- replaceId(
    data = conditionOccurrence,
    useNewId = assignNewId
  )
  conditionEra <- replaceId(
    data = conditionEra,
    useNewId = assignNewId
  )
  observation <- replaceId(
    data = observation,
    useNewId = assignNewId
  )
  procedureOccurrence <- replaceId(
    data = procedureOccurrence,
    useNewId = assignNewId
  )
  drugExposure <- replaceId(
    data = drugExposure,
    useNewId = assignNewId
  )
  drugEra <- replaceId(data = drugEra, useNewId = assignNewId)
  measurement <- replaceId(
    data = measurement,
    useNewId = assignNewId
  )

  results <- list(
    cohort = cohort,
    person = person,
    subjects = subjects,
    observationPeriod = observationPeriod,
    visitOccurrence = visitOccurrence,
    conditionOccurrence = conditionOccurrence,
    conditionEra = conditionEra,
    observation = observation,
    procedureOccurrence = procedureOccurrence,
    drugExposure = drugExposure,
    drugEra = drugEra,
    measurement = measurement,
    conceptId = conceptIds,
    cohortName = cohortName,
    assignNewId = assignNewId,
    shiftDates = shiftDates,
    sampleSize = sampleSize,
    sampleFound = nrow(subjects)
  )

  dir.create(
    path = file.path(exportFolder, "CohortExplorer"),
    showWarnings = FALSE,
    recursive = TRUE
  )
  dir.create(
    path = file.path(exportFolder, "CohortExplorer", "data"),
    showWarnings = FALSE,
    recursive = TRUE
  )
  dir.create(
    path = file.path(exportFolder, "CohortExplorer", "R"),
    showWarnings = FALSE,
    recursive = TRUE
  )
  dir.create(
    path = file.path(exportFolder, "CohortExplorer", "renv"),
    showWarnings = FALSE,
    recursive = TRUE
  )

  file.copy(
    from = system.file("shiny", "CohortExplorer.Rproj", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "CohortExplorer.Rproj")
  )
  file.copy(
    from = system.file("shiny", "global.R", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "global.R")
  )
  file.copy(
    from = system.file("shiny", "ui.R", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "ui.R")
  )
  file.copy(
    from = system.file("shiny", "server.R", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "server.R")
  )
  file.copy(
    from = system.file("shiny", "renv.lock", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "renv.lock")
  )
  file.copy(
    from = system.file("shiny", ".Rprofile", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", ".Rprofile")
  )
  file.copy(
    from = system.file("shiny", "R", "widgets.R", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "R", "widgets.R")
  )
  file.copy(
    from = system.file("shiny", "R", "private.R", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "R", "private.R")
  )
  file.copy(
    from = system.file("shiny", "renv.lock", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "renv.lock")
  )
  file.copy(
    from = system.file("shiny", "renv", ".gitignore", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "renv", ".gitignore")
  )
  file.copy(
    from = system.file("shiny", "renv", "activate.R", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "renv", "activate.R")
  )
  file.copy(
    from = system.file("shiny", "renv", "settings.dcf", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "renv", "settings.dcf")
  )
  file.copy(
    from = system.file("shiny", "README.md", package = utils::packageName()),
    to = file.path(exportFolder, "CohortExplorer", "README.md")
  )

  ParallelLogger::logInfo(paste0("Writing ", rdsFileName))
  saveRDS(
    object = results,
    file = file.path(exportFolder, "CohortExplorer", "data", rdsFileName)
  )

  delta <- Sys.time() - startTime
  ParallelLogger::logInfo(
    " - Extracting person level data took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}
