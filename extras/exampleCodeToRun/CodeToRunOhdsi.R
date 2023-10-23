# SETUP --------------------------------------------------------------------
library(magrittr)

# Pre-requisites ----
# remotes::install_github('OHDSI/CohortExplorer')

cohortDefinitionIds <- c(19, 200)

cohortDefinitionSet <-
  PhenotypeLibrary::getPlCohortDefinitionSet(cohortIds = PhenotypeLibrary::getPhenotypeLog()$cohortId)

exportFolder <-
  "d:/studyResults/CohortExplorer/phenotypeLibraryRealData"

databaseIds <-
  c(
    'truven_ccae',
    'truven_mdcd'
    # ,
    # 'cprd',
    # 'jmdc',
    # 'optum_extended_dod',
    # 'optum_ehr',
    # 'truven_mdcr',
    # 'ims_australia_lpd',
    # 'ims_germany',
    # 'ims_france',
    # 'iqvia_amb_emr',
    # 'iqvia_pharmetrics_plus'
  )

for (i in (1:length(databaseIds))) {
  for (j in (1:length(cohortDefinitionIds))) {
    
    if (i == 2) {
      shiftDates = TRUE
    } else {
      shiftDates = FALSE
    }
    
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
    
    cohortTableName <- paste0(stringr::str_squish("pl_"),
                              stringr::str_squish(cdmSource$sourceKey))
    
    # EXECUTE --------------------------------------------------------------------
    
    CohortExplorer::createCohortExplorerApp(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = as.character(cdmSource$cohortDatabaseSchemaFinal),
      cdmDatabaseSchema = as.character(cdmSource$cdmDatabaseSchemaFinal),
      vocabularyDatabaseSchema = as.character(cdmSource$cdmDatabaseSchemaFinal),
      cohortTable = cohortTableName,
      cohortDefinitionId = cohortDefinitionIds[[j]],
      cohortName = cohortDefinitionSet |> 
        dplyr::filter(cohortId == cohortDefinitionIds[[j]]) |> 
        dplyr::pull(cohortName),
      exportFolder = exportFolder,
      databaseId = SqlRender::snakeCaseToCamelCase(cdmSource$database),
      shiftDate = shiftDates,
      featureCohortDatabaseSchema = as.character(cdmSource$cohortDatabaseSchemaFinal),
      featureCohortTable = cohortTableName,
      featureCohortDefinitionSet = cohortDefinitionSet
    )
  }
}
