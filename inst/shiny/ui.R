shinyUI(fluidPage(titlePanel(
  sprintf(
    "Exploring %s (%s) in %s.%s",
    cohortName,
    cohortDefinitionId,
    cohortDatabaseSchema,
    cohortTable
  )
),
fluidRow(
  column(
    2,
    tags$label(class = "control-label", `for` = "subjectId", "Subject ID"),
    textOutput("subjectId"),
    tags$label(class = "control-label", `for` = "age", "Age at First Index Date"),
    textOutput("age"),
    tags$label(class = "control-label", `for` = "gender", "Gender"),
    textOutput("gender"),
    actionButton("previousButton", "<"),
    actionButton("nextButton", ">"),
    checkboxGroupInput(
      "cdmTables",
      label = "CDM Table",
      choices = SqlRender::camelCaseToTitleCase(tables),
      selected = SqlRender::camelCaseToTitleCase(c(
        "visitOccurrence", "conditionOccurrence", "drugEra", "procedureOccurrence", "measurement", "observation"
      ))
    ),
    textAreaInput(
      "filterRegex",
      div("Concept Name Filter (keep)", actionLink("filterInfo", "", icon = icon("info-circle"))),
      placeholder = "regex"
    ),
    textAreaInput(
      "deleteRegex",
      div("Concept Name Filter (remove)", actionLink("filterInfo", "", icon = icon("info-circle"))),
      placeholder = "regex"
    ),
    dateRangeInput(
      inputId = "dateRangeFilter",
      label = "Filter date range",
      start = "1900-01-01",
      end = "2099-12-31"
    ),
    numericInput(inputId = "daysFromCohortStart", 
                 label = "Absolute days from Start", 
                 min = 0, max = 9999, 
                 step = 1, 
                 value = 9999),
    numericInput(inputId = "daysToCohortStart", 
                 label = "Absolute days to Start", 
                 min = 0, max = 9999, 
                 step = 1, 
                 value = 9999),
    shinyWidgets::pickerInput(inputId = "highlightConceptSet", 
                label = "Concept set", 
                choices = conceptSets$id$fullName,
                selected = NULL, 
                multiple = TRUE),
    checkboxInput("showPlot", "Show Plot", value = TRUE),
    checkboxInput("showTable", "Show Table", value = TRUE),
    checkboxInput("shiftDates", "Shift Dates", value = FALSE),
    checkboxInput("showSourceCode", "Show source code", value = FALSE),
  ),
  column(
    10,
    conditionalPanel(
      condition = "input.showTable==1",
      conditionalPanel(condition = "input.showPlot==1",
                       plotlyOutput("plotSmall", height = "400px")),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = "eventTable")),
      csvDownloadButton(ns = "eventTable", "eventTable")
    ),
    conditionalPanel(condition = "input.showTable==0 & input.showPlot==1",
                     plotlyOutput("plotBig", height = "800px"), )
  )
)))
