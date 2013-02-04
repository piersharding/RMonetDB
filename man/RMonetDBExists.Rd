\name{RMonetDBExists}
\alias{RMonetDBExists}
\alias{readCube}
\title{Check table exists}
\description{
  Run a check to see if a table exists
}
\usage{
RMonetDBExists(con, tablename)
}%"
\arguments{
  \item{con}{an Open MonetDB Connection handle}
  \item{tablename}{The name of a table to check}
}
\details{

  \preformatted{
  con = RMonetDBConnect("db.yml")

  res <- RMonetDBExists(con, "craftsmen")
  print(res)

  RMonetDBClose(con)
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
  res <- RMonetDBExists(con, "craftsmen")
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
