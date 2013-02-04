\name{RMonetDBCommit}
\alias{RMonetDBCommit}
\title{execute a COMMIT transaction call}
\description{
  Execute an SQL COMMIT transaction call.
}
\usage{
RMonetDBCommit(con)
}%"
\arguments{
  \item{con}{an Open MonetDB Connection handle}
}
\details{

  RMonetDBCommit(con)

}

\note{
  Not much to note here.
}

\value{
  Returns true or false
}
\examples{\dontrun{
# Commit the open transaction
RMonetDBCommit(con)
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
