\name{RMonetDBExecute}
\alias{RMonetDBExecute}
\alias{dbexecute}
\title{Execute SQL against MonetDB}
\description{
  Execute arbitrary SQL statements against the MonetDB database
}
\usage{
RMonetDBExecute(conn, "SELECT * FROM voc.craftsmen LIMIT 3")
RMonetDBExecute(conn, "SELECT * FROM voc.craftsmen LIMIT 3", autocommit=TRUE, lastid=FALSE, try=FALSE)
}%"
\arguments{
  \item{conn}{an Open MonetDB Connection handle}
  \item{SQL}{SQL query to call}
  \item{autocommit}{optional - set autocommit for the statement to true}
  \item{lastid}{optional - return the record ID of the record affected - useful for INSERT}
  \item{try}{optional - try executing the statement - return FALSE on failure - TRUE for success}
}
\details{

  conn <- RMonetDBConnect("db.yml")
  res = RMonetDBExecute(conn, "SELECT * FROM voc.craftsmen LIMIT 3")
  print(res)
  checkTrue(as.numeric(res) == 3)
  RMonetDBClose(conn)

}

\note{
  nothing.
}

\value{
  RMonetDBExecute executes and returns the count of records affected 
}

\examples{\dontrun{
# Execute the query
RMonetDBExecute(conn, "SELECT * FROM voc.craftsmen LIMIT 3")
}}

\author{
  Piers Harding
}
\seealso{
  \code{\link{RMonetDB}}, \code{\link{RMonetDBConnect}}, \code{\link{RMonetDBGetInfo}}, \code{\link{RMonetDBQuery}}, \code{\link{RMonetDBLoadDataFrame}}, \code{\link{RMonetDBExecute}}, \code{\link{RMonetDBClose}}, \code{\link{RMonetDBStartTransaction}}, \code{\link{RMonetDBCommit}}, \code{\link{RMonetDBIsConnected}}, \code{\link{RMonetDBQuote}}, \code{\link{RMonetDBUnQuote}}, \code{\link{RMonetDBExists}}, \code{\link{RMonetDBListTables}}, \code{\link{RMonetDBListFields}}
}
\keyword{IO}
\keyword{SQL}
\keyword{MonetDB}
