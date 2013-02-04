\name{RMonetDBQuote}
\alias{RMonetDBQuote}
\title{Quote escape a string ready for SQL input}
\description{
  Quote escape a string ready for SQL input
}
\usage{
RMonetDBQuote(str)
}%"
\arguments{
  \item{str}{String requiring SQL escaping}
}
\details{

  res <- RMonetDBQuote("\'some&string-that%requires =looking at")
  print(res)

}

\note{
  Not much to note here.
}

\value{
  Returns string
}
\examples{\dontrun{
# Close the connection
RMonetDBQuote("\'some&string-that%requires =looking at")
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
