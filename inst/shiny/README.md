CohortExplorer
==============

How to use
==========

- The output of createCohortExplorerApp is a Shiny App with person level data in .rds. It is in the output folder.
- Go the output location in your file browser (e.g. windows file explorer in a Windows computer) and start 'CohortExplorer.Rproj'.
- In R console now run renv::restore() to enable renv. This will download all required packages and dependencies and set up the run environment. 
- Next call to shiny::runApp() 
- If you want to run this Shiny App on a remote Shiny Server, you may copy all the files in the output folder to the remote shiny servers new app file folder. run renv::restore() in the shiny server and restart app.


License
=======
CohortExplorer is licensed under Apache License 2.0