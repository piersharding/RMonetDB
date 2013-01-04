\name{RMonetDBListTables}
\alias{RMonetDBListTables}
\title{List tables}
\description{
      List tables available in the current MonetDB connection
}
\usage{
RMonetDBListTables(conn)
}%"
\arguments{
  \item{conn}{an Open MonetDB Connection handle}
}
\details{

  conn <- RMonetDBConnect("db.yml")
  res <- RMonetDBListTables(conn)
  print(res)
  RMonetDBClose(conn)

}

\note{
  Not much to note here.
}

\value{
  Returns true or false
}
\examples{\dontrun{
# List tables
RMonetDBListTables(conn)
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
