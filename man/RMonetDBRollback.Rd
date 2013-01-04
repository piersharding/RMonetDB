\name{RMonetDBRollback}
\alias{RMonetDBRollback}
\title{execute a ROLLBACK transaction call}
\description{
  Execute an SQL ROLLBACK transaction call.
}
\usage{
RMonetDBRollback(conn)
}%"
\arguments{
  \item{conn}{an Open MonetDB Connection handle}
}
\details{

  RMonetDBRollback(conn)

}

\note{
  Not much to note here.
}

\value{
  Returns true or false
}
\examples{\dontrun{
# Rollback the open transaction
RMonetDBRollback(conn)
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
