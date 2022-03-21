#### Common Functions
### Date Created: 5/29/2020
### Author: Daniel Shields daniel.shields@abbott.com
### Notes: Machine must have Java (one of the versions listed below) in order for mailR to work

### Updated: 03/21/2022 (daniel.shields@abbott.com) - cleaning up and commentin the code to be more shareable and readable.



#set Java Home for mailR
if(file.exists('C:\\Program Files\\Java\\jre1.8.0_321')) {
  Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_321')
} else {
  Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_291')
}


# library(mailR)
# library(odbc)
# olapR needs to be manually copied to the local package directory.  Does not work in R 4.0 (04/12/2021)
#library(olapR)



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
postDataToBOA <- function(df, dfname) {
  #connection to the data defined
  con1 <- dbConnect(odbc::odbc(),
                    .connection_string = odbcConnStrBOA)
  #write table to SQL Server
  dbWriteTable(con1,
               dfname,
               df,
               overwrite = TRUE)
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

