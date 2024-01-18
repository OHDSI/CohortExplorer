# Copyright 2024 Observational Health Data Sciences and Informatics
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

# Format and check code ---------------------------------------------------
styler::style_pkg()
OhdsiRTools::checkUsagePackage("CohortExplorer")
OhdsiRTools::updateCopyrightYearFolder()

# Devtools check -----------------------------------------------------------
devtools::spell_check()
devtools::check()

# Create manual -----------------------------------------------------------
unlink("extras/CohortExplorer.pdf")
shell("R CMD Rd2pdf ./ --output=extras/CohortExplorer.pdf")
dir.create(path = file.path("inst", "doc"), showWarnings = FALSE, recursive = TRUE)

# Create Vignettes---------------------------------------------------------
rmarkdown::render("vignettes/HowToUseCohortExplorer.Rmd",
                  output_file = "../inst/doc/HowToUseCohortExplorer.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))


# Build site---------------------------------------------------------
devtools::document()
pkgdown::build_site()
OhdsiRTools::fixHadesLogo()



# Release package to CRAN ------------------------------------------------------
devtools::check_win_devel()
devtools::check_rhub()
devtools::release()
devtools::check(cran=TRUE)
