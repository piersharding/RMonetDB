\name{RMonetDBUnQuote}
\alias{RMonetDBUnQuote}
\title{Unquote an SQL encoded string}
\description{
  Unquote an SQL encoded string
}
\usage{
RMonetDBUnQuote(str)
}%"
\arguments{
  \item{str}{string that has been quote escaped for SQL}
}
\details{

  res <- RMonetDBUnQuote("\\' escaped\& string \\t")
  print(res)

}

\note{
  Not much to note here.
}

\value{
  Returns true or false
}

\examples{\dontrun{
# un escape quote escaped string for SQL
RMonetDBUnQuote("\\' escaped\& string \\t")
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
