### Azure SQL Server SQL, Azure SSAS Cube MDX, Connecting with R Lunch and Learn Example with Sourcing the Connections
### Date Created: 5/29/2020
### Updates: 04/12/2021 - shieldx1
### Author: Daniel Shields daniel.shields@abbott.com


# Load Packages ----------------------------------------------------------
# Commentted uncommon packages to install for various reasons ------------

#install.packages("installr")
#library(installr)
#updateR()



#install.packages("taskscheduleR")
#library(taskscheduleR)
#taskscheduleR:::taskschedulerAddin()

#set Java Home for mailR
if(file.exists('C:\\Program Files\\Java\\jre1.8.0_321')) {
  Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_321')  
} else {
  Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_291')
}

library(mailR)
library(tidyverse)
library(readtext)
library(lubridate)
library(odbc)
library(plotly)
# olapR needs to be manually copied to the local package directory.  Does not work in R 4.0 (04/12/2021)
#library(olapR)
#library(RDCOMClient)



# source connections file
source("C:/Users/shieldx1/OneDrive - Abbott/LoginInfo/Connections.R")

# Functions ---------------------------------------------------------------

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

