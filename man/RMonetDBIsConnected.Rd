\name{RMonetDBIsConnected}
\alias{RMonetDBIsConnected}
\title{Check connection to MonetDB}
\description{
  Check the viability of a connection to MonetDB
}
\usage{
RMonetDBIsConnected(conn, "funciton_name", parms)
}%"
\arguments{
  \item{conn}{an Open MonetDB Connection handle}
}
\details{

  conn <- RMonetDBConnect("db.yml")
  res <- RMonetDBIsConnected(conn)
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
# Check the connection
RMonetDBIsConnected(conn)
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
