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



#' Export person level data for cohort
#'
#' @description
#' Export person level data from omop cdm tables from eligible persons in the cohort.
#'
#' @template ConnectionDetails
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
#' @param shiftDates                  (Default = TRUE) Do you want to shift dates? This will help further de-identify data. The shift
#'                                    is the process of recalibrating dates such that all persons min(observation_period_start_date) is
#'                                    0000-01-01.
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
#' exportPersonLevelData(
#'   connectionDetails = connectionDetails,
#'   cohortDefinitionId = 1234
#' )
#' }
#'
#' @export
exportPersonLevelData <-
  function(connectionDetails,
           cohortDatabaseSchema = "cohort",
           cdmDatabaseSchema,
           vocabularyDatabaseSchema = cdmDatabaseSchema,
           tempEmulationSchema = NULL,
           cohortTable = "cohort",
           cohortDefinitionId,
           cohortName = NULL,
           sampleSize = 25,
           personIds = NULL,
           exportFolder,
           databaseId,
           shiftDates = TRUE) {
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
      len = 1,
      null.ok = TRUE,
      add = errorMessage
    )
    checkmate::reportAssertions(collection = errorMessage)

    originalDatabaseId <- databaseId
    databaseId <-
      as.character(gsub(
        pattern = " ",
        replacement = "",
        x = databaseId
      ))
    databaseId <-
      as.character(gsub(
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

    exportFolder <- normalizePath(exportFolder, mustWork = FALSE)

    errorMessage <-
      createIfNotExist(
        type = "folder",
        name = exportFolder,
        errorMessage = errorMessage
      )

    rdsFileName <-
      paste0(
        "CohortExplorer_",
        cohortDefinitionId,
        "_",
        databaseId,
        ".RData"
      )

    if (file.exists(file.path(exportFolder, rdsFileName))) {
      warning(paste0("Found previous ", rdsFileName, ". This will be replaced."))
    }

    ## Set up connection to server----
    ParallelLogger::logTrace(" - Setting up connection")
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))


    if (!is.null(personIds)) {
      persons <- dplyr::tibble(personId = personIds)
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
    } else {
      # take a random sample
      sql <- "SELECT TOP @sample_size subject_id person_id
                INTO #persons_filter
                FROM (
                    	SELECT DISTINCT subject_id
                    	FROM @cohort_database_schema.@cohort_table
                    	WHERE cohort_definition_id = @cohort_definition_id
                	) all_ids
                ORDER BY NEWID();"

      writeLines("Attempting to find subjects in cohort table.")
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        sample_size = sampleSize,
        cohort_database_schema = cohortDatabaseSchema,
        cohort_table = cohortTable,
        cohort_definition_id = cohortDefinitionId
      )
    }


    writeLines("Getting cohort table.")
    cohort <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT c.subject_id,
              	cohort_start_date,
              	cohort_end_date
              FROM @cohort_database_schema.@cohort_table c
              INNER JOIN #persons_filter p
              ON c.subject_id = p.person_id
              WHERE cohort_definition_id = @cohort_definition_id
          ORDER BY c.subject_id, cohort_start_date;",
        cohort_database_schema = cohortDatabaseSchema,
        cohort_table = cohortTable,
        cohort_definition_id = cohortDefinitionId,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble() %>%
      dplyr::arrange(subjectId, cohortStartDate)

    subjectIdsFound <- unique(cohort$subjectId)

    if (nrow(cohort) == 0) {
      warning("Cohort does not have the selected subject ids")
      return(NULL)
    }

    writeLines("Getting person table.")
    person <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT p.person_id,
                gender_concept_id,
                year_of_birth
        FROM @cdm_database_schema.person p
        INNER JOIN #persons_filter pf
        ON p.person_id = pf.person_id
        ORDER BY p.person_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble()

    person <- person %>%
      dplyr::inner_join(
        cohort %>%
          dplyr::group_by(subjectId) %>%
          dplyr::summarise(yearOfCohort = min(clock::get_year(cohortStartDate)), .groups = "keep") %>%
          dplyr::ungroup() %>%
          dplyr::rename("personId" = subjectId),
        by = "personId"
      ) %>%
      dplyr::mutate(age = yearOfCohort - yearOfBirth) %>%
      dplyr::select(-yearOfCohort, -yearOfBirth)

    writeLines("Getting observation period table.")
    observationPeriod <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT op.person_id,
                    observation_period_start_date,
                    observation_period_end_date,
                    period_type_concept_id
              FROM @cdm_database_schema.observation_period op
              INNER JOIN #persons_filter p
              ON op.person_id = p.person_id
              ORDER BY op.person_id,
                      observation_period_start_date,
                      observation_period_end_date;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble()

    writeLines("Getting visit occurrence table.")
    visitOccurrence <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT v.person_id,
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
                  visit_start_date,
                  visit_end_date,
                  visit_concept_id,
                  visit_type_concept_id,
                  visit_source_concept_id
        ORDER BY v.person_id,
                visit_start_date,
                visit_end_date,
                visit_concept_id,
                visit_type_concept_id,
                visit_source_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble()

    writeLines("Getting condition occurrence table.")
    conditionOccurrence <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT c.person_id,
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
                  condition_start_date,
                  condition_end_date,
                  condition_concept_id,
                  condition_type_concept_id,
                  condition_source_concept_id
        ORDER BY c.person_id,
                  condition_start_date,
                  condition_end_date,
                  condition_concept_id,
                  condition_type_concept_id,
                  condition_source_concept_id;",
        cdm_database_schema = cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()

    writeLines("Getting condition era table.")
    conditionEra <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT ce.person_id,
              condition_era_start_date AS start_date,
              condition_era_end_date AS end_date,
              condition_concept_id AS concept_id,
          	  count(*) records
        FROM @cdm_database_schema.condition_era ce
        INNER JOIN #persons_filter p
        ON ce.person_id = p.person_id
        GROUP BY ce.person_id,
              condition_era_start_date,
              condition_era_end_date,
              condition_concept_id
        ORDER BY ce.person_id,
              condition_era_start_date,
              condition_era_end_date,
              condition_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble() %>%
      dplyr::mutate(typeConceptId = 0, records = 1)

    writeLines("Getting observation table.")
    observation <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT o.person_id,
              observation_date AS start_date,
              observation_concept_id AS concept_id,
          	  observation_type_concept_id AS type_concept_id,
          	  observation_source_concept_id AS source_concept_id,
          	  count(*) records
        FROM @cdm_database_schema.observation o
        INNER JOIN #persons_filter p
        ON o.person_id = p.person_id
        GROUP BY o.person_id,
                  observation_date,
                  observation_concept_id,
                  observation_type_concept_id,
                  observation_source_concept_id
        ORDER BY o.person_id,
                observation_date,
                observation_concept_id,
                observation_type_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble()

    writeLines("Getting procedure occurrence table.")
    procedureOccurrence <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT p.person_id,
              procedure_date AS start_date,
              procedure_concept_id AS concept_id,
              procedure_type_concept_id AS type_concept_id,
              procedure_source_concept_id AS source_concept_id,
          	  count(*) records
        FROM @cdm_database_schema.procedure_occurrence p
        INNER JOIN #persons_filter pf
        ON p.person_id = pf.person_id
        GROUP BY p.person_id,
                  procedure_date,
                  procedure_concept_id,
                  procedure_type_concept_id,
                  procedure_source_concept_id
        ORDER BY p.person_id,
                procedure_date,
                procedure_concept_id,
                procedure_type_concept_id,
                procedure_source_concept_id;",
        cdm_database_schema = cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble() %>%
      dplyr::mutate(endDate = startDate)

    writeLines("Getting drug exposure table.")
    drugExposure <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT de.person_id,
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
                  drug_exposure_start_date,
                  drug_exposure_end_date,
                  drug_concept_id,
                  drug_type_concept_id,
                  drug_source_concept_id
        ORDER BY de.person_id,
                  drug_exposure_start_date,
                  drug_exposure_end_date,
                  drug_concept_id,
                  drug_type_concept_id,
                  drug_source_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble()

    writeLines("Getting drug era table.")
    drugEra <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT de.person_id,
              drug_era_start_date AS start_date,
              drug_era_end_date AS end_date,
              drug_concept_id AS concept_id,
          	  count(*) records
        FROM @cdm_database_schema.drug_era de
        INNER JOIN #persons_filter pf
        ON de.person_id = pf.person_id
        GROUP BY de.person_id,
                  drug_era_start_date,
                  drug_era_end_date,
                  drug_concept_id
        ORDER BY de.person_id,
                  drug_era_start_date,
                  drug_era_end_date,
                  drug_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble() %>%
      dplyr::mutate(typeConceptId = 0)

    writeLines("Getting measurement table.")
    measurement <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT m.person_id,
              measurement_date AS start_date,
              measurement_concept_id AS concept_id,
              measurement_type_concept_id as type_concept_id,
              measurement_source_concept_id as source_concept_id,
          	  count(*) records
        FROM @cdm_database_schema.measurement m
        INNER JOIN #persons_filter pf
        ON m.person_id = pf.person_id
        GROUP BY m.person_id,
                  measurement_date,
                  measurement_concept_id,
                  measurement_type_concept_id,
                  measurement_source_concept_id
        ORDER BY m.person_id,
                  measurement_date,
                  measurement_concept_id,
                  measurement_type_concept_id,
                  measurement_source_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble() %>%
      dplyr::mutate(endDate = startDate)


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
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble()

    cohort <- cohort %>%
      dplyr::rename(personId = subjectId)

    subjects <- cohort %>%
      dplyr::group_by(personId) %>%
      dplyr::summarise(cohortStartDate = min(cohortStartDate)) %>%
      dplyr::inner_join(person,
        by = "personId"
      ) %>%
      dplyr::inner_join(conceptIds,
        by = c("genderConceptId" = "conceptId")
      ) %>%
      dplyr::rename(gender = conceptName) %>%
      dplyr::ungroup()

    if (shiftDates) {
      originDate <- as.Date("0000-01-01")
      personMinObservationPeriodDate <- observationPeriod %>%
        dplyr::group_by(personId) %>%
        dplyr::summarise(minObservationPeriodDate = min(observationPeriodStartDate), .groups = "keep") %>%
        dplyr::ungroup()

      observationPeriod <- observationPeriod %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(observationPeriodStartDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = observationPeriodStartDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::mutate(observationPeriodEndDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = observationPeriodEndDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)

      cohort <- cohort %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(cohortStartDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = cohortStartDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::mutate(cohortEndDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = cohortEndDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)

      conditionEra <- conditionEra %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(startDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = startDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::mutate(endDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = endDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)

      conditionOccurrence <- conditionOccurrence %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(startDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = startDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::mutate(endDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = endDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)

      drugEra <- drugEra %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(startDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = startDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::mutate(endDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = endDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)

      drugExposure <- drugExposure %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(startDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = startDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::mutate(endDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = endDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)

      measurement <- measurement %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(startDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = startDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::mutate(endDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = endDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)

      observation <- observation %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(startDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = startDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)

      procedureOccurrence <- procedureOccurrence %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(startDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = startDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::mutate(endDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = endDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)

      visitOccurrence <- visitOccurrence %>%
        dplyr::inner_join(personMinObservationPeriodDate,
          by = "personId"
        ) %>%
        dplyr::mutate(startDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = startDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::mutate(endDate = clock::add_days(
          x = as.Date(originDate),
          n = as.integer(
            difftime(
              time1 = endDate,
              time2 = minObservationPeriodDate,
              units = "days"
            )
          )
        )) %>%
        dplyr::select(-minObservationPeriodDate)
    }

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
      cohortName = cohortName
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
      from = system.file("shiny", "R", "widgets.R", package = utils::packageName()),
      to = file.path(exportFolder, "CohortExplorer", "R", "widgets.R")
    )
    file.copy(
      from = system.file("shiny", "R", "private.R", package = utils::packageName()),
      to = file.path(exportFolder, "CohortExplorer", "R", "private.R")
    )

    if (file.exists(file.path(exportFolder, "data", rdsFileName))) {
      unlink(
        x = file.path(exportFolder, "data", rdsFileName),
        recursive = TRUE,
        force = TRUE
      )
    }

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
