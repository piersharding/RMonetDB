\name{RMonetDBExists}
\alias{RMonetDBExists}
\alias{readCube}
\title{Check table exists}
\description{
  Run a check to see if a table exists
}
\usage{
RMonetDBExists(conn, "table name")
}%"
\arguments{
  \item{conn}{an Open MonetDB Connection handle}
  \item{table name}{The name of a table to check}
}
\details{

  \preformatted{
  conn = RMonetDBConnect("db.yml")

  res <- RMonetDBExists(conn, "craftsmen")
  print(res)

  RMonetDBClose(conn)
  }
}

\note{
  nothing
}

\value{
  Returns boolean true or false depending on whether the table exists or not
}
\examples{\dontrun{
# Check if the voc.craftsmen table exists
  res <- RMonetDBExists(conn, "craftsmen")
  print(res)
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
