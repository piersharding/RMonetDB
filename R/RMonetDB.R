# file RMonetDB/R/RMonetDB.R
# copyright (C) 2012 and onwards, Piers Harding
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 or 3 of the License
#  (at your option).
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  A copy of the GNU General Public License is available at
#  http://www.r-project.org/Licenses/
#
#  Function library of mapi integration for MonetDB
#
#
#
.onLoad <- function(libname, pkgname)
{
    if(is.null(getOption("dec")))
        options(dec = Sys.localeconv()["decimal_point"])
}

.onUnload <- function(libpath)
{
    .Call(C_RMonetDBTerm)
    library.dynam.unload("RMonetDB", libpath)
}


RMonetDBConnect <- function (...)
{
    args <- list(...)
    if (length(args) == 0) {
        stop("No arguments supplied")
    }
    if (typeof(args[[1]]) == "list") {
        args = args[[1]]
    }
    # did we get passed a config file?
    if (typeof(args[[1]]) == "character" && file.exists(args[[1]])) {
        library(yaml)
        config <- yaml.load_file(args[[1]])
        newargs <- list()
        for (x in names(config)) { newargs[[x]] <- as.character(config[[x]]); }
        return(RMonetDBConnect(newargs))
    }
    # format dbport
    if (exists("dbport", where=args)) {
        args[['dbport']] <- sprintf("%05d", as.integer(args[['dbport']]))
    }
    res <- .Call(C_RMonetDBConnect, args)
    return(res)
}

RMonetDBshowArgs <-
    function(...)
{
    res <- .Call(C_RMonetDBshowArgs, list(...))
    return(res)
}

RMonetDBIsConnected <-  function(handle)
{
    if (!is.integer(handle)) {
        #print("RMonetDBIsConnected: handle is not an integer")
        return(FALSE)
    }
    if (!is.element("handle_ptr", names(attributes(handle)))) {
        #print("RMonetDBIsConnected: handle_ptr does not exist")
        return(FALSE)
    }
    res <- .Call(C_RMonetDBIsConnected, handle)
    return(res)
}


RMonetDBGetInfo <- function(handle)
{
    if(!RMonetDBIsConnected(handle))
       stop("RMonetDBGetInfo: argument is not a valid RMonetDB handle")
    res <- .Call(C_RMonetDBGetInfo, handle)
    return(res)
}


RMonetDBExecute <- function(handle, sql, autocommit=TRUE, lastid=FALSE, try=FALSE)
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a valid RMonetDB handle")
    res <- .Call(C_RMonetDBExecute, handle, sql, autocommit, lastid, try)
    return(res)
}


RMonetDBStartTransaction <- function(handle)
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a valid RMonetDB handle")
	res <- RMonetDBExecute(handle, "START TRANSACTION", autocommit=TRUE)
    return(res)
}


RMonetDBCommit <- function(handle)
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a valid RMonetDB handle")
	res <- RMonetDBExecute(handle, "COMMIT", autocommit=FALSE)
    return(res)
}


RMonetDBRollback <- function(handle)
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a valid RMonetDB handle")
	res <- RMonetDBExecute(handle, "ROLLBACK", autocommit=FALSE)
    return(res)
}


RMonetDBExists <- function(handle, tablename)
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a valid RMonetDB handle")
    exists <- paste('SELECT * FROM "', tablename, '" WHERE 1=0', sep="")
	res <- RMonetDBExecute(handle, exists, autocommit=FALSE, try=TRUE)
    return(res)
}


RMonetDBListTables <- function(handle)
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a valid RMonetDB handle")
    sql <- "SELECT \"o\".\"name\" AS name, (CASE \"o\".\"type\" WHEN 0 THEN 'TABLE' WHEN 1 THEN 'VIEW' ELSE '' END) AS \"type\", \"o\".\"query\" AS query FROM \"sys\".\"_tables\" o, \"sys\".\"schemas\" s WHERE \"o\".\"schema_id\" = \"s\".\"id\" AND \"o\".\"type\" IN (0,1) AND \"s\".\"name\" = \"current_schema\""
	res <- RMonetDBQuery(handle, sql, autocommit=FALSE)
    return(res)
}


RMonetDBListFields <- function(handle, tablename, schema="")
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a valid RMonetDB handle")
    if (nchar(schema) > 0) {
        schema <- paste(" AND \"s\".\"name\" = '", schema, "' ", sep="")
    }
    else {
        schema <- ""
    }
    #sql <- paste("SELECT * FROM \"", tablename, "\" LIMIT 1", sep="")
    sql <- paste("SELECT \"s\".\"name\" AS \"schema\", \"t\".\"name\" AS \"table\", \"c\".\"name\" AS \"column\", \"c\".\"type\", \"c\".\"type_digits\", \"c\".\"type_scale\", \"c\".\"null\", \"c\".\"default\", \"c\".\"number\" FROM \"sys\".\"_columns\" \"c\", \"sys\".\"_tables\" \"t\", \"sys\".\"schemas\" \"s\" WHERE \"c\".\"table_id\" = \"t\".\"id\" AND '", tablename, "' = \"t\".\"name\"", schema, " AND \"t\".\"schema_id\" = \"s\".\"id\" ORDER BY \"schema\", \"number\"", sep="")
	res <- RMonetDBQuery(handle, sql, autocommit=FALSE)
    return(res)
}


RMonetDBQuery <- function(handle, sql, parameters=list(), autocommit=FALSE, lastid=FALSE, page=FALSE)
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a valid RMonetDB handle")
	parameters <- as.list(as.character(parameters))
    res <- .Call(C_RMonetDBQuery, handle, sql, parameters, autocommit, lastid, page)
    return(res)
}

rmonetdb_col <- function(name, typ, len)
{
    if (typ == 'integer') {
        typ = 'integer'
    }
    else if (typ == 'numeric' || typ == 'double') {
        typ = 'real'
    }
    else {
        if (len == 1) {
            len = 1
        }
        else if (len < 5) {
            len = 5
        }
        else if (len < 10) {
            len = 10
        }
        else if (len < 20) {
            len = 20
        }
        else if (len < 50) {
            len = 50
        }
        else if (len < 100) {
            len = 100
        }
        typ = paste('varchar(', len, ')', sep='')
    }
    return(paste('"', name,'" ', typ, ' DEFAULT NULL', sep=''))
}


rmonetdb_val <- function(val)
{
    typ <- typeof(val)
    if (typ == 'integer' || typ == 'numeric' || typ == 'double') {
        val = as.character(val)
    }
    else {
        val = paste('"', RMonetDBQuote(val), '"', sep='')
    }
    return(val)
}


RMonetDBLoadDataFrame <- function(handle, table, tablename, drop=FALSE, chunk=100000)
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a valid RMonetDB handle")
	table <- as.data.frame(table)
	tablename <- as.character(tablename)
    cols <- paste(lapply(names(table), FUN=function (x) {return(rmonetdb_col(x, typeof(table[[x]]), max(unlist(lapply(as.character(table[[x]]), FUN=nchar)))))}), collapse=", ")
    create <- paste('CREATE TABLE "', tablename, '" (', cols, ')', sep="") 
    there <- RMonetDBExists(handle, tablename)
    # wrap the whole thing in a single transaction
    RMonetDBStartTransaction(handle)
    if (drop) {
        # check if table exists first
        if (there) {
            delete <- paste('DROP TABLE "', tablename, '"', sep="")
	        res <- RMonetDBExecute(handle, delete, autocommit=FALSE)
        }
    }

	res <- RMonetDBExecute(handle, create, autocommit=FALSE)

    # calculate the values for the rows
    rows <- length(table[[1]])

    # must have some rows
    if (rows == 0) {
        return(res)
    }

    # work out how many chunks
    iter <- ceiling(rows/chunk)
    tot <- 0

    for (j in 1:iter) {
        start = ((j - 1) * chunk) + 1
        end = j * chunk
        if (end > rows) 
            end = rows
        #print(paste("start: ", start))
        #print(paste("end: ", end))
        query <- c(paste("COPY ", (end - start + 1), ' RECORDS INTO "', tablename, '" FROM stdin USING DELIMITERS \'\\t\',\'\\n\',\'"\';', "\n", sep=""))
        for (i in start:end) {
            query <- append(query, paste(paste(lapply(table[i,], FUN=rmonetdb_val), collapse="\t"), "\n", sep=""))
        }
        query <- paste(query, collapse="")
	    res <- RMonetDBExecute(handle, query, autocommit=FALSE)
        tot = tot + res
    }
    RMonetDBCommit(handle)

    return(res)
}


RMonetDBQuote <- function(str)
{
    res <- .Call(C_RMonetDBQuote, str)
    return(res)
}


RMonetDBUnQuote <- function(str)
{
    res <- .Call(C_RMonetDBUnQuote, str)
    return(res)
}

print.RMonetDB_Connector <- function(conn, ...) RMonetDBGetInfo(conn)

close.RMonetDB_Connector <- function(conn, ...) RMonetDBClose(conn)

RMonetDBClose <- function(handle)
{
    if(!RMonetDBIsConnected(handle))
       stop("argument is not a connected RMonetDB handle")
    res <- .Call(C_RMonetDBClose, handle)
    return(res)
}

if (!exists("dbwrite")) {
    dbwrite <- function(conn, ...){
              res <-  RMonetDBLoadDataFrame(conn, ...)
              return(res)
          }
}

if (!exists("dbquery")) {
    dbquery <- function(conn, ...){
              res <-  RMonetDBQuery(conn, ...)
              return(res)
          }
}

if (!exists("dbexecute")) {
    dbexecute <- function(conn, ...){
              res <-  RMonetDBExecute(conn, ...)
              return(res)
          }
}

setClass("RMonetDB_Connector")
setMethod("dbwrite", "RMonetDB_Connector",
          def = function(conn, ...){
              res <-  RMonetDBLoadDataFrame(conn, ...)
              return(res)
          },
          valueClass = "data.frame"
        )
setMethod("dbquery", "RMonetDB_Connector",
          def = function(conn, ...){
              res <-  RMonetDBQuery(conn, ...)
              return(res)
          },
          valueClass = "data.frame"
        )
setMethod("dbexecute", "RMonetDB_Connector",
          def = function(conn, ...){
              res <-  RMonetDBExecute(conn, ...)
              return(res)
          },
          valueClass = "data.frame"
        )

