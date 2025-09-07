## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----echo = FALSE, out.width = "100%", fig.cap = "Table 1: Table of rcprd functions"----
#![Table 1: Table of rcprd functions](FunctionTable20240906.png){width = 100%}
knitr::include_graphics("FunctionTable20240906.png")

## -----------------------------------------------------------------------------
#devtools::install_github("alexpate30/rcprd")
#install.packages("rcprd") NOT YET ON CRAN
library(rcprd)
#devtools::load_all()
list.files(system.file("aurum_data", package = "rcprd"), pattern = ".txt")

## -----------------------------------------------------------------------------
pat <- extract_cohort(filepath = system.file("aurum_data", package = "rcprd"), patids = as.character(c(1,3,4,6)))
str(pat)

## -----------------------------------------------------------------------------
pat <- extract_cohort(filepath = system.file("aurum_data", package = "rcprd"))
str(pat)

## -----------------------------------------------------------------------------
prac <- extract_practices(filepath = system.file("aurum_data", package = "rcprd"))
str(prac)

## -----------------------------------------------------------------------------
pat <- merge(pat, prac, by.x = "pracid", by.y = "pracid")

## -----------------------------------------------------------------------------
pat <- subset(pat, patid %in% c(1,3,4,6))

## -----------------------------------------------------------------------------
aurum_extract <- connect_database(file.path(tempdir(), "temp.sqlite"))

## -----------------------------------------------------------------------------
add_to_database(filepath = system.file("aurum_data", "aurum_allpatid_set1_extract_observation_001.txt", package = "rcprd"), 
                filetype = "observation", subset_patids = c(1,3,4,6), db = aurum_extract, overwrite = TRUE)
add_to_database(filepath = system.file("aurum_data", "aurum_allpatid_set1_extract_observation_002.txt", package = "rcprd"), 
                filetype = "observation", subset_patids = c(1,3,4,6), db = aurum_extract, append = TRUE)
add_to_database(filepath = system.file("aurum_data", "aurum_allpatid_set1_extract_observation_003.txt", package = "rcprd"), 
                filetype = "observation", subset_patids = c(1,3,4,6), db = aurum_extract, append = TRUE)

## -----------------------------------------------------------------------------
db_query(db_open = aurum_extract, tab = "observation", n = 3)

## -----------------------------------------------------------------------------
add_to_database(filepath = system.file("aurum_data", "aurum_allpatid_set1_extract_drugissue_001.txt", package = "rcprd"), 
                filetype = "drugissue", subset_patids = c(1,3,4,6), db = aurum_extract, overwrite = TRUE)
add_to_database(filepath = system.file("aurum_data", "aurum_allpatid_set1_extract_drugissue_002.txt", package = "rcprd"), 
                filetype = "drugissue", subset_patids = c(1,3,4,6), db = aurum_extract, append = TRUE)
add_to_database(filepath = system.file("aurum_data", "aurum_allpatid_set1_extract_drugissue_003.txt", package = "rcprd"), 
                filetype = "drugissue", subset_patids = c(1,3,4,6), db = aurum_extract, append = TRUE)

## -----------------------------------------------------------------------------
db_query(db_open = aurum_extract, tab = "drugissue", n = 3)

## -----------------------------------------------------------------------------
RSQLite::dbListTables(aurum_extract)

## -----------------------------------------------------------------------------
RSQLite::dbDisconnect(aurum_extract)

## -----------------------------------------------------------------------------
aurum_extract <- connect_database(file.path(tempdir(), "temp.sqlite"))

## -----------------------------------------------------------------------------
### Extract data
cprd_extract(db = aurum_extract, 
             filepath = system.file("aurum_data", package = "rcprd"), 
             filetype = "observation", subset_patids = c(1,3,4,6), use_set = FALSE)

### Query first three rows
db_query(db_open = aurum_extract, tab = "observation", n = 3)

## -----------------------------------------------------------------------------
### Extract data
cprd_extract(db = aurum_extract, 
             filepath = system.file("aurum_data", package = "rcprd"), 
             filetype = "drugissue", subset_patids = c(1,3,4,6), use_set = FALSE)

### List tables
RSQLite::dbListTables(aurum_extract)

### Query first three rows
db_query(db_open = aurum_extract, tab = "drugissue", n = 3)

### Disconnect
RSQLite::dbDisconnect(aurum_extract)

## -----------------------------------------------------------------------------
pat <- extract_cohort(filepath = system.file("aurum_data", package = "rcprd"), patids = as.character(c(1,3,4,6)), set = TRUE)
pat

## -----------------------------------------------------------------------------
### Create connection to SQLite database
aurum_extract <- connect_database(file.path(tempdir(), "temp.sqlite"))

### Add observation files
cprd_extract(db = aurum_extract, 
             filepath = system.file("aurum_data", package = "rcprd"), 
             filetype = "observation", 
             subset_patids = pat, 
             use_set = TRUE)

### Add drugissue files
cprd_extract(db = aurum_extract, 
             filepath = system.file("aurum_data", package = "rcprd"), 
             filetype = "drugissue", 
             subset_patids = pat, 
             use_set = TRUE)

### Query first three rows of each table
db_query(db_open = aurum_extract, tab = "observation", n = 3)
db_query(db_open = aurum_extract, tab = "drugissue", n = 3)

## -----------------------------------------------------------------------------
### Define codelist
my_vector_codelist <- "187341000000114"

### Add an index date to cohort
pat$fup_start <- as.Date("01/01/2020", format = "%d/%m/%Y")

### Extract a history of type variable using extract_ho
ho <- extract_ho(cohort = pat, 
                 codelist_vector = my_vector_codelist, 
                 indexdt = "fup_start", 
                 db_open = aurum_extract, 
                 tab = "observation",
                 return_output = TRUE)
ho

## -----------------------------------------------------------------------------
### Add an censoring date to cohort
pat$fup_end <- as.Date("01/01/2024", format = "%d/%m/%Y")

### Extract a time until variable using extract_time_until
time_until <- extract_time_until(cohort = pat, 
                                 codelist_vector = my_vector_codelist, 
                                 indexdt = "fup_start", 
                                 censdt = "fup_end",
                                 db_open = aurum_extract, 
                                 tab = "observation",
                                 return_output = TRUE)
time_until

## -----------------------------------------------------------------------------
### Extract test data using extract_test_data
test_data <- extract_test_data(cohort = pat, 
                          codelist_vector = my_vector_codelist, 
                          indexdt = "fup_start", 
                          db_open = aurum_extract,
                          time_post = 0,
                          time_prev = Inf,
                          return_output = TRUE)
test_data

## -----------------------------------------------------------------------------
### Recursive merge
analysis.ready.pat <- Reduce(function(df1, df2) merge(df1, df2, by = "patid", all.x = TRUE), list(pat[,c("patid", "gender", "yob")], ho, time_until, test_data)) 
analysis.ready.pat

## -----------------------------------------------------------------------------
my_codelist_df <- data.frame("condition" = "mycondition", medcodeid = c("221511000000115", "187341000000114"), "subgroup" = c("subgroup1", "subgroup2"))

extract_test_data(cohort = pat,
                  codelist_df = subset(my_codelist_df, subgroup == "subgroup1"),
                  indexdt = "fup_start",
                  db_open = aurum_extract,
                  time_post = 0,
                  time_prev = Inf,
                  return_output = TRUE)

extract_test_data(cohort = pat,
                  codelist_df = subset(my_codelist_df, subgroup == "subgroup2"),
                  indexdt = "fup_start",
                  db_open = aurum_extract,
                  time_post = 0,
                  time_prev = Inf,
                  return_output = TRUE)

## -----------------------------------------------------------------------------
my_db_query <- db_query(db_open = aurum_extract,
                        tab ="observation",
                        codelist_vector = "114311000006111")

my_db_query

## -----------------------------------------------------------------------------
### Add an index date to pat
pat$indexdt <- as.Date("01/01/2020", format = "%d/%m/%Y")

### Combine query with cohort creating a boolean variable denoting 'history of'
combine.query.boolean <- combine_query_boolean(cohort = pat,
                                               db_query = my_db_query,
                                               query_type = "med")
  
combine.query.boolean

## -----------------------------------------------------------------------------
### Combine query with cohort retaining most recent three records
combine.query <- combine_query(cohort = pat,
                               db_query = my_db_query,
                               query_type = "med",
                               numobs = 3,
                               reduce_output = TRUE)
  
combine.query

## -----------------------------------------------------------------------------
### Extract a history of type variable using extract_ho
combine.query <- combine_query(cohort = pat,
                               db_query = my_db_query,
                               query_type = "test",
                               numobs = 3,
                               reduce_output = TRUE)
  
combine.query

### Disconnect
RSQLite::dbDisconnect(aurum_extract)

## ----include = FALSE----------------------------------------------------------
### Hide clean up of filespaces prior to moving onto next session (clean up required by CRAN)
unlink(file.path(tempdir(), "temp.sqlite"))

## ----include = FALSE----------------------------------------------------------
## Save working directory location, so can revert at end of vignette (required by CRAN)
oldwd <- getwd()

## -----------------------------------------------------------------------------
## Set working directory
knitr::opts_knit$set(root.dir = tempdir())

## -----------------------------------------------------------------------------
suppressMessages(
  create_directory_system()
)

file.exists(file.path(tempdir(), "data"))
file.exists(file.path(tempdir(), "codelists"))
file.exists(file.path(tempdir(), "code"))

## -----------------------------------------------------------------------------
## Open connection
aurum_extract <- connect_database("data/sql/mydb.sqlite")

## Add data to SQLite database using cprd_extract
cprd_extract(db = aurum_extract,
             filepath = system.file("aurum_data", package = "rcprd"),
             filetype = "observation", use_set = FALSE)

## Disconnect
RSQLite::dbDisconnect(aurum_extract)

## -----------------------------------------------------------------------------
### Define codelist
my_codelist <- data.frame(medcodeid = "187341000000114")

### Save codelist
write.csv(my_codelist, "codelists/analysis/mylist.csv")

## -----------------------------------------------------------------------------
extract_ho(cohort = pat,
           codelist = "mylist",
           indexdt = "fup_start",
           db = "mydb",
           tab = "observation",
           return_output = FALSE,
           out_save_disk = TRUE)

## -----------------------------------------------------------------------------
readRDS("data/extraction/var_ho.rds")

## ----include = FALSE----------------------------------------------------------
### Return filespace to how it was prior to example
delete_directory_system()

## ----include = FALSE----------------------------------------------------------
## Return root directory to how it was prior to example
knitr::opts_knit$set(root.dir = oldwd)

