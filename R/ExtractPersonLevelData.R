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



#' Extract person level data for cohort
#'
#' @description
#' Given an instantiated cohort table, extract all person level data from omop cdm tables
#' for persons found in the cohort.
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
#' @param cohortDefinitionId              The cohort id to extract records.
#'
#' @param sampleSize            (Optional, default = 20) The number of persons to randomly sample. Ignored, if personId is given.
#'
#' @param personIds              (Optional) An array of personId's to look for in Cohort table and CDM.
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
#' extractaPersonLevelData(
#'   connectionDetails = connectionDetails,
#'   cohortDefinitionId = 1234
#' )
#' }
#'
#' @export
extractaPersonLevelData <-
  function(connectionDetails,
           cohortDatabaseSchema = "cohort",
           cdmDatabaseSchema,
           vocabularyDatabaseSchema = cdmDatabaseSchema,
           tempEmulationSchema = NULL,
           cohortTable = "cohort",
           cohortDefinitionId,
           sampleSize = 25,
           personIds = NULL) {
    startTime <- Sys.time()
    
    errorMessage <- checkmate::makeAssertCollection()
    
    checkmate::assertCharacter(x = cohortDatabaseSchema,
                               min.len = 1,
                               add = errorMessage)
    
    checkmate::assertCharacter(x = cdmDatabaseSchema,
                               min.len = 1,
                               add = errorMessage)
    
    checkmate::assertCharacter(x = vocabularyDatabaseSchema,
                               min.len = 1,
                               add = errorMessage)
    
    checkmate::assertCharacter(x = cohortTable,
                               min.len = 1,
                               add = errorMessage)
    
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
        sql = "SELECT subject_id,
              	cohort_start_date,
              	cohort_end_date
              FROM @cohort_database_schema.@cohort_table c
              INNER JOIN #persons_filter p
              ON c.subject_id = p.person_id
              WHERE cohort_definition_id = @cohort_definition_id
          ORDER BY subject_id, cohort_start_date;",
        cohort_database_schema = shinySettings$cohortDatabaseSchema,
        cohort_table = shinySettings$cohortTable,
        cohort_definition_id = shinySettings$cohortDefinitionId,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble() %>%
      dplyr::arrange(subjectId, cohortStartDate)
    
    shinySettings$subjectIdsFound <- unique(cohort$subjectId)
    
    if (nrow(cohort) == 0) {
      stop("Cohort does not have the selected subject ids")
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
        ORDER BY person_id;",
      cdm_database_schema = shinySettings$cdmDatabaseSchema,
      subject_ids = shinySettings$subjectIds,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble()
    
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
      cdm_database_schema = shinySettings$cdmDatabaseSchema,
      subject_ids = shinySettings$subjectIds,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble()
    
    writeLines("Getting visit occurrence table.")
    visitOccurrence <-  DatabaseConnector::renderTranslateQuerySql(
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
      cdm_database_schema = shinySettings$cdmDatabaseSchema,
      subject_ids = shinySettings$subjectIds,
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
        WHERE person_id IN (@subject_ids)
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
        cdm_database_schema = shinySettings$cdmDatabaseSchema,
        subject_ids = shinySettings$subjectIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()
    
    writeLines("Getting condition era table.")
    conditionEra <-  DatabaseConnector::renderTranslateQuerySql(
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
      cdm_database_schema = shinySettings$cdmDatabaseSchema,
      subject_ids = shinySettings$subjectIds,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble() %>%
      dplyr::mutate(typeConceptId = 0, records = 1)
    
    writeLines("Getting observation table.")
    observation <-  DatabaseConnector::renderTranslateQuerySql(
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
      cdm_database_schema = shinySettings$cdmDatabaseSchema,
      subject_ids = shinySettings$subjectIds,
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
        cdm_database_schema = shinySettings$cdmDatabaseSchema,
        subject_ids = shinySettings$subjectIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble() %>%
      dplyr::mutate(endDate = startDate)
    
    writeLines("Getting drug exposure table.")
    drugExposure <-  DatabaseConnector::renderTranslateQuerySql(
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
      cdm_database_schema = shinySettings$cdmDatabaseSchema,
      subject_ids = shinySettings$subjectIds,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble()
    
    writeLines("Getting drug era table.")
    drugEra <-  DatabaseConnector::renderTranslateQuerySql(
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
      cdm_database_schema = shinySettings$cdmDatabaseSchema,
      subject_ids = shinySettings$subjectIds,
      snakeCaseToCamelCase = TRUE
    ) %>%
      dplyr::tibble() %>%
      dplyr::mutate(typeConceptId = 0)
    
    writeLines("Getting measurement table.")
    measurement <-  DatabaseConnector::renderTranslateQuerySql(
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
        WHERE person_id IN (@subject_ids)
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
      cdm_database_schema = shinySettings$cdmDatabaseSchema,
      subject_ids = shinySettings$subjectIds,
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
    
    
    delta <- Sys.time() - startTime
    ParallelLogger::logInfo(" - Extracting person level data took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }