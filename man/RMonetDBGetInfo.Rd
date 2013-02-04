\name{RMonetDBGetInfo}
\alias{RMonetDBGetInfo}
\title{get connection details to MonetDB}
\description{
  get connection details regarding the nominated MonetDB connection
}
\usage{
RMonetDBGetInfo(con)
}%"
\arguments{
  \item{con}{an Open MonetDB Connection handle}
}
\details{
  \code{RMonetDBGetInfo} get connection details 
}

\note{
  Not much to note here.
}

\value{
  Returns a named list of details about the connection
}
\examples{\dontrun{
# Close the connection
info <- RMonetDBGetInfo(con)
print(info)
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
