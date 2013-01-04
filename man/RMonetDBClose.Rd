\name{RMonetDBClose}
\alias{RMonetDBClose}
\title{MonetDB Close database connections}
\description{
  Close an open connection to a MonetDB Database
}
\usage{
RMonetDBClose(conn)
}%"
\arguments{
  \item{conn}{an Open MonetDB Connection handle}
}
\details{
  \code{RMonetDBClose} closes aspecific connection to a MonetDB database
}

\note{
  Not much to note here.
}

\value{
  Returns true or false
}

\author{
  Piers Harding
}
\seealso{
  \code{\link{RMonetDB}}, \code{\link{RMonetDBConnect}}, \code{\link{RMonetDBGetInfo}}, \code{\link{RMonetDBQuery}}, \code{\link{RMonetDBLoadDataFrame}}, \code{\link{RMonetDBExecute}}, \code{\link{RMonetDBClose}}, \code{\link{RMonetDBStartTransaction}}, \code{\link{RMonetDBCommit}}, \code{\link{RMonetDBIsConnected}}, \code{\link{RMonetDBQuote}}, \code{\link{RMonetDBUnQuote}}, \code{\link{RMonetDBExists}}, \code{\link{RMonetDBListTables}}, \code{\link{RMonetDBListFields}}
}
\keyword{IO}
\keyword{SQL}
\keyword{MonetDB}
