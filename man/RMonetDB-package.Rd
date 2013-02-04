\name{RMonetDB-package}
\alias{RMonetDB}
\title{MonetDB SQL Interface}

\description{
  Package \pkg{RMonetDB} MonetDB SQL Interface implemented using the MAPI client library.

  See the package manual for details of installation and use.  (This
  will show up as a vignette, and can be accessed \emph{via}
  \code{RShowDoc("RMonetDB", package="RMonetDB")}.)
}

\details{
  Read and write to tables in the MonetDB in-memory column store DB, using
  SQL over the MonetDB MAPI client library

  con <- RMonetDBConnect("db.yml")

  or 

  con <- RMonetDBConnect(dbhost="localhost",
                          dbuser="voc",
                          dbpass="voc",
                          dbname="voc")

  info = RMonetDBGetInfo(con)
  print(info)

  parms <- list(3, 50)
  qry <- "SELECT * FROM voc.craftsmen WHERE trip > ? LIMIT ?"

  res <- RMonetDBQuery(con, qry, parms)
  print(res)
  # alternative shorthand
  res <- dbquery(con, qry, parms)
  print(res)
  RMonetDBClose(con)

  All Query results are returned as data.frame.
}

\note{
  Please see the README and INSTALL file that come with RMonetDB - these
  will give the latest info on how to install and test the package, 
  inparticular details are given on how to setup the test database and 
  run the test suite.
}

\author{
  Piers Harding
}
\seealso{
  \code{\link{RMonetDB}}, \code{\link{RMonetDBConnect}}, \code{\link{RMonetDBGetInfo}}, \code{\link{RMonetDBQuery}}, \code{\link{RMonetDBLoadDataFrame}}, \code{\link{RMonetDBExecute}}, \code{\link{RMonetDBClose}}, \code{\link{RMonetDBStartTransaction}}, \code{\link{RMonetDBCommit}}, \code{\link{RMonetDBRollback}}, \code{\link{RMonetDBIsConnected}}, \code{\link{RMonetDBQuote}}, \code{\link{RMonetDBUnQuote}}, \code{\link{RMonetDBExists}}, \code{\link{RMonetDBListTables}}, \code{\link{RMonetDBListFields}}
}
\keyword{IO}
\keyword{SQL}
\keyword{MonetDB}
