# Copyright 2023 Observational Health Data Sciences and Informatics
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

replaceId <- function(data, useNewId = TRUE) {
  if (useNewId) {
    data <- data %>%
      dplyr::select(-"personId") %>%
      dplyr::rename("personId" = newId)
  } else {
    data <- data %>%
      dplyr::select(-"newId")
  }
  return(data)
}


takeRandomSample <- function(x, size) {
  if (length(x) <= 1) {
    return(x |> as.double())
  } else {
    return(sample(
      x = x,
      size = size,
      replace = FALSE
    )) |> as.double()
  }
}

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