CohortExplorer
================


[![Build Status](https://github.com/OHDSI/CohortExplorer/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CohortExplorer/actions?query=workflow%3AR-CMD-check)
[![codecov.io](https://codecov.io/github/OHDSI/CohortExplorer/coverage.svg?branch=main)](https://codecov.io/github/OHDSI/CohortExplorer?branch=main)

Introduction
============

An R package with a Shiny viewer to explore profiles of patients in a cohort. The output of this R-package is a self contained R shiny that contain person level data for review. 

Warning
========

- Contains person level data. This package is not to be considered de-identified.
- Please do not share the output with others as it may violate protected health information.

Features
========

- From an instantiated cohort, identifies specified number of random persons. It also allows for non random selection by specifying a set of personId as input.
- Extracts person level data for each person from the common data model, and constructs a results object in rds form. This rds object has person level data with personId and dates.
- Accepts a set of configurable parameters for the shiny application. This parameters will be chosen in the shiny app. e.g. regular expression.
- Allows additional de-identification with shifting dates and newId, ie.. shifts all dates so that the first observation_period_start_date for a person is set to January 1st 1900, and all other dates are shifted in relation to this date. Also creates and replaces the source personId with a new randomly generated id.
- Creates a R shiny app in a specified local folder (zipped), that can then be published to a shiny server or explored locally.

Technology
============
CohortExplorer is an R package.

System Requirements
============
Requires R (version 3.6.0 or higher). 

Installation
=============
1. See the instructions [here](https://ohdsi.github.io/Hades/rSetup.html) for configuring your R environment, including RTools and Java.

2. In R, use the following commands to download and install CohortExplorer:

  ```r
  install.packages("remotes")
  remotes::install_github("ohdsi/CohortExplorer")
  ```

User Documentation
==================
Documentation can be found on the [package website](https://ohdsi.github.io/CohortExplorer).

PDF versions of the documentation are also available:
* Package manual: [CohortExplorer.pdf](https://raw.githubusercontent.com/OHDSI/CohortExplorer/main/extras/CohortExplorer.pdf)

Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/CohortExplorer/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

Contributing
============
Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.

License
=======
CohortExplorer is licensed under Apache License 2.0

Development
===========
CohortExplorer is being developed in R Studio.

### Development status

CohortExplorer is under development.
