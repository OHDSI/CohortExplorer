# SETUP --------------------------------------------------------------------
library(magrittr)
# Pre-requisites ----
remotes::install_github('OHDSI/CohortExplorer')

cohortDefinitionIds <- c(10393)

ROhdsiWebApi::authorizeWebApi(baseUrl = Sys.getenv("BaseUrl"), authMethod = "windows")
cohortDefinitionSet <-
  ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = Sys.getenv("BaseUrl")) %>%
  dplyr::filter(id %in% c(cohortDefinitionIds)) %>%
  dplyr::select(id, name) %>%
  dplyr::rename(cohortId = id, cohortName = name) %>%
  dplyr::arrange(cohortId)

exportFolder <- "c:/temp/CohortExplorer"
projectCode <- "epi1024CohortDiagnostics"


######################################################################################
############## databaseIds to run cohort diagnostics on that source  #################
######################################################################################

databaseIds <-
  c(
    'truven_ccae',
    'truven_mdcd',
    'cprd',
    'jmdc',
    'optum_extended_dod',
    'optum_ehr',
    'truven_mdcr',
    'ims_australia_lpd',
    'ims_germany',
    'ims_france',
    'iqvia_amb_emr',
    'iqvia_pharmetrics_plus'
  )

for (i in (1:length(databaseIds))) {
  for (j in (1:length(cohortDefinitionIds))) {
    cdmSource <- cdmSources %>%
      dplyr::filter(.data$sequence == 1) %>%
      dplyr::filter(database == databaseIds[[i]])
    
    connectionDetails <-
      DatabaseConnector::createConnectionDetails(
        dbms = cdmSource$dbms,
        server = as.character(cdmSource$serverFinal),
        user = keyring::key_get(service = 'OHDSI_USER'),
        password = keyring::key_get(service = 'OHDSI_PASSWORD'),
        port = cdmSource$port
      )
    
    cohortTableName <- paste0(stringr::str_squish(projectCode),
                              stringr::str_squish(cdmSource$sourceId))
    
    # EXECUTE --------------------------------------------------------------------
    CohortExplorer::createCohortExplorerApp(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = as.character(cdmSource$cohortDatabaseSchemaFinal),
      cdmDatabaseSchema = as.character(cdmSource$cdmDatabaseSchemaFinal),
      vocabularyDatabaseSchema = as.character(cdmSource$cdmDatabaseSchemaFinal),
      cohortTable = cohortTableName,
      cohortDefinitionId = cohortDefinitionSet[j, ]$cohortId,
      cohortName = cohortDefinitionSet[j, ]$cohortName,
      exportFolder = exportFolder,
      databaseId = SqlRender::snakeCaseToCamelCase(cdmSource$database),
      shiftDate = TRUE
    )
  }
}
