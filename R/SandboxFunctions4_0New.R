#### Common Functions
### Date Created: 5/29/2020
### Author: Daniel Shields daniel.shields@abbott.com
### Notes: Machine must have Java (one of the versions listed below) in order for mailR to work

### Updated: 03/21/2022 (daniel.shields@abbott.com) - cleaning up and commentin the code to be more shareable and readable.
### Updated: 05/17/2022 (rachel.addlespurger@abbott.com) - adding function used for restated date selection by week used in Edge/Vendor Central scripts.
### Updated: 01/20/2023 (daniel.shields@abbott.com) - adding a fastLoadUtil to load data to our BOA quickly.  Also cleaning up some of the script (java definitions)

if(!require(mailR)){
  install.packages("mailR",dependencies = TRUE)
  library(mailR)
}

if(!require(odbc)){
  install.packages("odbc",dependencies = TRUE)
  library(odbc)
}

#library(mailR)
#library(odbc)
# olapR needs to be manually copied to the local package directory.  Does not work in R 4.0 (04/12/2021)
#library(olapR)

#Test

#### Functions ---------------------------------------------------------------


# execute query on sql server - no return
qrySandboxExecute <- function(sqlText1) {
  #collapse any potential lists sent into character values before execute.
  sqlText1 = paste(sqlText1, collapse = '')
  #connection to the data defined
  con <- dbConnect(odbc::odbc(),
                   .connection_string = odbcConnStr)
  #query sent
  input <- dbGetQuery(con, sqlText1)
}



# function defined to query the sandbox - return a dataframe
qrySandboxReturn <- function(sqlText1) {
  #collapse any potential lists sent into character values before execute.
  sqlText1 = paste(sqlText1, collapse = '')
  #connection to the data defined
  con <- dbConnect(odbc::odbc(),
                   .connection_string = odbcConnStr)
  #query sent
  outputData <- dbGetQuery(con, sqlText1)
  #return query output
  return(outputData)
}



# function defined to post data to the sandbox
postDataToSandbox <- function(df, dfname) {
  #connection to the data defined
  con1 <- dbConnect(odbc::odbc(),
                    .connection_string = odbcConnStr)
  #write table to SQL Server
  dbWriteTable(con1,
               dfname,
               df,
               overwrite = TRUE)
}



# function defined to post data to the sandbox with optional fieldtypes
postDataToSandboxv2 <- function(df, dfname, fieldtypes = NULL) {
  #connection to the data defined
  con2 <- dbConnect(odbc::odbc(),
                    .connection_string = odbcConnStr)
  #if fieldtypes is in the input parameters, include it in the statement
  if (missing(fieldtypes)) {
    dbWriteTable(con2,
                 dfname,
                 df,
                 overwrite = TRUE)
  } else {
    dbWriteTable(con2,
                 dfname,
                 df,
                 overwrite = TRUE,
                 field.types = fieldtypes)
  }
}


# function defined to post data as append to the sandbox with optional fieldtypes
postDataToSandboxAppend <- function(df, dfname, fieldtypes = NULL){
  #connection to the data defined
  con2 <- dbConnect(odbc::odbc(),
                    .connection_string = odbcConnStr)
  #if fieldtypes is in the input parameters, include it in the statement
  if(missing(fieldtypes)){
    dbWriteTable(con2,
                 dfname,
                 df,
                 overwrite = FALSE,
                 append = TRUE)
  } else {
    dbWriteTable(con2,
                 dfname,
                 df,
                 overwrite = FALSE,
                 append = TRUE,
                 field.types = fieldtypes)
  }
}


# execute query on sql server - no return
qryBOAExecute <- function(sqlText1) {
  #collapse any potential lists sent into character values before execute.
  sqlText1 = paste(sqlText1, collapse = '')
  #connection to the data defined
  con <- dbConnect(odbc::odbc(),
                   .connection_string = odbcConnStrBOA)
  #query sent
  input <- dbGetQuery(con, sqlText1)
}



# function defined to query the sandbox - return a dataframe
qryBOAReturn <- function(sqlText1) {
  #collapse any potential lists sent into character values before execute.
  sqlText1 = paste(sqlText1, collapse = '')
  #connection to the data defined
  con <- dbConnect(odbc::odbc(),
                   .connection_string = odbcConnStrBOA)
  #query sent
  outputData <- dbGetQuery(con, sqlText1)
  #return query output
  return(outputData)
}



# function defined to post data to the sandbox
postDataToBOA2 <- function(df, tblname, schma) {
  #connection to the data defined
  con1 <- dbConnect(odbc::odbc(),
                    .connection_string = odbcConnStrBOA)

  #write table to SQL Server
  dbWriteTable(con1,
               name=DBI::Id(schema=schma,table=tblname),
               df,
               overwrite = TRUE)
}


# function defined to post data to the BOA; appending to tables vs. overwriting
postDataToBOAAppend <- function(df, tblname, schma) {
  #connection to the data defined
  con1 <- dbConnect(odbc::odbc(),
                    .connection_string = odbcConnStrBOA)

  #write table to SQL Server
  dbWriteTable(con1,
               name=DBI::Id(schema=schma,table=tblname),
               df,
               overwrite = FALSE,
               append = TRUE)
}



# function defined to post data to the sandbox with optional fieldtypes
postDataToBOAv2 <- function(df, dfname, fieldtypes = NULL) {
  #connection to the data defined
  con2 <- dbConnect(odbc::odbc(),
                    .connection_string = odbcConnStrBOA)
  #if fieldtypes is in the input parameters, include it in the statement
  if (missing(fieldtypes)) {
    dbWriteTable(con2,
                 dfname,
                 df,
                 overwrite = TRUE)
  } else {
    dbWriteTable(con2,
                 dfname,
                 df,
                 overwrite = TRUE,
                 field.types = fieldtypes)
  }
}



# function to fastload large data to the BOA
# will drop table, then append a list of dataframes together into the target schema.tblname
fastLoadUtil <- function (df, rows_to_chop_by, tblname, schma, LogFilePath)
{

  #suppress messages and ensure packages necessary are loaded for users calling this function
  suppressPackageStartupMessages(require(dplyr))
  suppressPackageStartupMessages(require(lubridate))
  require(DBI)
  require(devtools)
  require(CommonFunctions)
  require(Connections)

  #define the functions from this package to be used inside of this function; to reduce repeated code
  .GlobalEnv$LogEvent <- CommonFunctions::LogEvent
  .GlobalEnv$qryBOAExecute <- CommonFunctions::qryBOAExecute
  .GlobalEnv$odbcConnStrBOA <- Connections::odbcConnStrBOA
  .GlobalEnv$postDataToBOAAppend <- CommonFunctions::postDataToBOAAppend

  LogFile <- LogFilePath
  rowsinfile <- rows_to_chop_by
  myDataFileCount <- as.integer((nrow(df)/rowsinfile)+1)
  myData <- list()

 Start_time <- Sys.time()

  i <- 1
  #loop to filter and populate the list
  while (i <= myDataFileCount)
  {

    myData[[i]] <- df %>%
      filter((row(df) <= (rowsinfile*i)) & (row(df) > (rowsinfile*(i-1))))

    LogEvent(paste0("myData list build #:", i, " of ", myDataFileCount), LogFile)

    i <- i + 1

  }

  #drop the table in case it's there.
  LogEvent(paste0("Drop the table ", schma, ".", tblname, "in case it's there:"), LogFile)
  sql <- paste0("DROP TABLE IF EXISTS ", schma, ".", tblname, ";")
  qryBOAExecute(sql)

  #loop through the list of Dataframes to load these dataframes to a table on the BOA
  LogEvent(paste0("Start the SQL Server Load loop:"), LogFile)

  c <- 1
  #load loop
  while (c <= myDataFileCount)
  {

    LogEvent(paste0("Load file ", c, " of ", myDataFileCount, " Begin: "), LogFile)

    #write table to SQL Server
    postDataToBOAAppend(myData[[c]], tblname, schma)

    LogEvent(paste0("Load file ", c, " of ", myDataFileCount, " Completed: "), LogFile)

    c <- c + 1

  }

  End_time <- Sys.time()
  message(nrow(df)," rows loaded in ",round(difftime(End_time, Start_time, units = "mins"), 2) , " mins. Loaded ", rows_to_chop_by, " rows at a time.")
  LogEvent(paste0(nrow(df)," rows loaded in ", round(difftime(End_time, Start_time, units = "mins"), 2), " mins. Loaded ", rows_to_chop_by, " rows at a time."), LogFile)

}


# function defined to post data to the BOA; appending to tables vs. overwriting
postDataToSynapseAppend <- function(df, tblname, schma) {
  #connection to the data defined
  con1 <- dbConnect(odbc::odbc(),
                    .connection_string = odbcConnStrSynpase)

  #write table to SQL Server
  dbWriteTable(con1,
               name=DBI::Id(schema=schma,table=tblname),
               df,
               overwrite = FALSE,
               append = TRUE)
}

# function defined to post data to the BOA; appending to tables vs. overwriting
postDataToSynapse <- function(df, tblname, schma) {
  #connection to the data defined
  con1 <- dbConnect(odbc::odbc(),
                    .connection_string = odbcConnStrSynpase)

  #write table to SQL Server
  dbWriteTable(con1,
               name=DBI::Id(schema=schma,table=tblname),
               df,
               overwrite = TRUE)
}

# execute query on Synapse server - no return
qrySynapseExecute <- function(sqlText1) {
  #collapse any potential lists sent into character values before execute.
  sqlText1 = paste(sqlText1, collapse = '')
  #connection to the data defined
  con <- dbConnect(odbc::odbc(),
                   .connection_string = odbcConnStrSynpase)
  #query sent
  input <- dbGetQuery(con, sqlText1)
}

# function defined to query the sandbox - return a dataframe
qrySynapseReturn <- function(sqlText1) {
  #collapse any potential lists sent into character values before execute.
  sqlText1 = paste(sqlText1, collapse = '')
  #connection to the data defined
  con <- dbConnect(odbc::odbc(),
                   .connection_string = odbcConnStrSynpase)
  #query sent
  outputData <- dbGetQuery(con, sqlText1)
  #return query output
  return(outputData)
}

fastLoadSynapseUtil <- function (df, rows_to_chop_by, tblname, schma, LogFilePath)
{

  #suppress messages and ensure packages necessary are loaded for users calling this function
  suppressPackageStartupMessages(require(dplyr))
  suppressPackageStartupMessages(require(lubridate))
  require(DBI)
  require(devtools)
  require(CommonFunctions)
  require(Connections)

  #define the functions from this package to be used inside of this function; to reduce repeated code
  .GlobalEnv$LogEvent <- LogEvent
  .GlobalEnv$qrySynapseExecute <- qrySynapseExecute
  .GlobalEnv$odbcConnStrBOA <- odbcConnStrSynpase
  .GlobalEnv$postDataToBOAAppend <- postDataToSynapseAppend

  LogFile <- LogFilePath
  rowsinfile <- rows_to_chop_by
  myDataFileCount <- as.integer((nrow(df)/rowsinfile)+1)
  myData <- list()

  Start_time <- Sys.time()

  i <- 1
  #loop to filter and populate the list
  while (i <= myDataFileCount)
  {

    myData[[i]] <- df %>%
      filter((row(df) <= (rowsinfile*i)) && (row(df) > (rowsinfile*(i-1))))

    LogEvent(paste0("myData list build #:", i, " of ", myDataFileCount), LogFile)

    i <- i + 1

  }

  #drop the table in case it's there.
  LogEvent(paste0("Drop the table ", schma, ".", tblname, "in case it's there:"), LogFile)
  sql <- paste0("IF OBJECT_ID ('[", schma, "].[", tblname,"]', 'U' ) IS NOT NULL DROP TABLE ", schma, ".", tblname,";")   ############################################################### RDA UPDATE
  qryBOAExecute(sql)

  #loop through the list of Dataframes to load these dataframes to a table on the BOA
  LogEvent(paste0("Start the SQL Server Load loop:"), LogFile)

  c <- 1
  #load loop
  while (c <= myDataFileCount)
  {

    LogEvent(paste0("Load file ", c, " of ", myDataFileCount, " Begin: "), LogFile)

    #write table to SQL Server
    postDataToBOAAppend(myData[[c]], tblname, schma)

    LogEvent(paste0("Load file ", c, " of ", myDataFileCount, " Completed: "), LogFile)

    c <- c + 1

  }

  End_time <- Sys.time()
  message(nrow(df)," rows loaded in ",round(difftime(End_time, Start_time, units = "mins"), 2) , " mins. Loaded ", rows_to_chop_by, " rows at a time.")
  LogEvent(paste0(nrow(df)," rows loaded in ", round(difftime(End_time, Start_time, units = "mins"), 2), " mins. Loaded ", rows_to_chop_by, " rows at a time."), LogFile)

}


##email function using mailR - no attachment
sendEmailNoAtt <- function (to, sendby, subject, body){
  send.mail(
    from = sendby,
    to = to,
    subject = subject,
    smtp = list(
      host.name = "mail.abbott.com",
      port = 25,
      ssl = FALSE
    ),
    body = body,
    send = TRUE,
    html = TRUE)
}

##email function using mailR - with attachment
sendEmailWAtt <- function (to, sendby, subject, body, attachment){
  send.mail(
    from = sendby,
    to = to,
    subject = subject,
    smtp = list(
      host.name = "mail.abbott.com",
      port = 25,
      ssl = FALSE
    ),
    body = body,
    attach.files = attachment,
    send = TRUE,
    html = TRUE)
}



#log Events
LogEvent <- function (LogString, LogFile) {
  log_con <- file(LogFile, open="a")
  log_string <- paste(now(), ": ", LogString)
  cat(log_string, file = log_con, sep="\n")
}

# Message2Log
Message2Log <- function (M)
{ #log <- paste0(directoryLoc,"/Log_Files/",Sys.Date(),".txt")
  #log_file <- file(log, open="a")
  log_string <- paste(now(), (M))
  cat(log_string, file = log_file, sep = "\n")
  message(M)
}

#date based on current date, number of weeks prior, and day of week
prevweekday <- function(date, wday) {
  date <- as.Date(date)
  diff <- wday - wday(date)
  if (diff > 0)
    diff <- diff - 7
  return(date + diff)
}

# TMC Functions ####

#Sum all
SumAll <- function(df) {
  output <- df %>%
    dplyr::group_by_if(negate(is.numeric)) %>%
    dplyr::summarise_if(is.numeric, sum, na.rm = TRUE) %>% ungroup(.)
}

cleanNAs <- function(df){
  sel <- names(df)
  df[sel] <-
    lapply(df[sel], function(x)
      replace(x, x %in% "N/A", NA))
  df
}

#Impact summary
impactsummaryfun <- function(sharetemptot_out, start_dt = NULL, end_dt = NULL) {if (missing(start_dt) | missing(end_dt)) {
  sharetemptot_out <- sharetemptot_out
} else {
  sharetemptot_out <- sharetemptot_out %>%
    filter(
      `Period Description` >= mdy(start_dt) &
        `Period Description` <= mdy(end_dt)
    )}
  sharetemptot_out <- sharetemptot_out %>%
    select(
      NIELSEN_SEGMENT,
      `Manufacturer`,
      `$`,
      `$ Previous`,
      `$ Diff`,
      `EQ`,
      `EQ Previous`,
      `EQ Diff`
    ) %>%
    SumAll(.) %>%
    group_by(NIELSEN_SEGMENT) %>%
    mutate(
      `$ Share` = round((`$` / sum(`$`)) * 100, digits = 2),
      `EQ Share` = round((`EQ` / sum(`EQ`)) * 100, digits = 2),
      `$ Share Previous` = round((`$ Previous` / sum(`$ Previous`)) *
                                   100, digits = 2),
      `EQ Share Previous` = round((`EQ Previous` / sum(`EQ Previous`)) *
                                    100, digits = 2),
      `$ Share Diff` = `$ Share` - `$ Share Previous`,
      `EQ Share Diff` = `EQ Share` - `EQ Share Previous`,
      `$ % Change` = round((`$ Diff` / `$ Previous`) * 100, digits = 2),
      `EQ % Change` = round((`EQ Diff` / `EQ Previous`) * 100, digits = 2),
      `$ % Tot Change` = round((`$ Diff` / sum(`$ Previous`)) * 100, digits = 2),
      `EQ % Tot Change` = round((`EQ Diff` / sum(`EQ Previous`)) * 100, digits = 2)
    ) %>%
    ungroup(.) %>%
    # filter(`$ Share Diff` != 0 |
    #          `EQ Share Diff` != 0 | `$ % Change` != 0 | `EQ % Change` != 0) %>%
    select(
      NIELSEN_SEGMENT,
      `Manufacturer`,
      `$`,
      `$ Previous`,
      `$ Diff`,
      `$ % Change`,
      `$ % Tot Change`,
      `$ Share`,
      `$ Share Previous`,
      `$ Share Diff`,
      `EQ`,
      `EQ Previous`,
      `EQ Diff`,
      `EQ % Change`,
      `EQ % Tot Change`,
      `EQ Share`,
      `EQ Share Previous`,
      `EQ Share Diff`
    )
}

# get objects
get.objects <- function(path2file = NULL, exception = NULL, source = FALSE, message = TRUE) {
  require("utils")
  require("tools")

  # Step 0-1: Possibility to leave path2file = NULL if using RStudio.
  # We are using rstudioapi to get the path to the current file
  if(is.null(path2file)) path2file <- rstudioapi::getSourceEditorContext()$path

  # Check that file exists
  if (!file.exists(path2file)) {
    stop("couldn't find file ", path2file)
  }

  # Step 0-2: If .Rmd file, need to extract the code in R chunks first
  # Use code in https://felixfan.github.io/extract-r-code/
  if(file_ext(path2file)=="Rmd") {
    require("knitr")
    tmp <- purl(path2file)
    path2file <- paste(getwd(),tmp,sep="/")
    source = TRUE # Must be changed to TRUE here
  }

  # Step 0-3: Start by running the script if you are calling an external script.
  if(source) source(path2file)

  # Step 1: screen the script
  summ_script <- getParseData(parse(path2file, keep.source = TRUE))

  # Step 2: extract the objects
  list_objects <- summ_script$text[which(summ_script$token == "SYMBOL")]
  # List unique
  list_objects <- unique(list_objects)

  # Step 3: find where the objects are.
  src <- paste(as.vector(sapply(list_objects, find)))
  src <- tapply(list_objects, factor(src), c)

  # List of the objects in the Global Environment
  # They can be in both the Global Environment and some packages.
  src_names <- names(src)

  list_objects = NULL
  for (i in grep("GlobalEnv", src_names)) {
    list_objects <- c(list_objects, src[[i]])
  }

  # Step 3bis: if any exception, remove from the list
  if(!is.null(exception)) {
    list_objects <- list_objects[!list_objects %in% exception]
  }

  # Step 4: done!
  # If message, print message:
  if(message) {
    cat(paste0("  ",length(list_objects)," objects  were created in the script \n  ", path2file,"\n"))
  }

  return(list_objects)
}
