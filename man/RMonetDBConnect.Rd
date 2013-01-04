\name{RMonetDBConnect}
\alias{RMonetDBConnect}
\title{RMonetDB Open Connections to the database}
\description{
  Open connections to a MonetDB database
}
\usage{
conn <- RMonetDBConnect(dbhost='localhost', dbuser='voc', dbpass='voc', dbname='voc')

or

RMonetDBConnect('db.yml')

}%"
\arguments{
  \item{...}{all MonetDB connection parameters for the MAPI client library}
  \item{'db.yml'}{A YAML encoded file containing MAPI client library connection parameters.}
}
\details{
  \code{RMonetDBConnect} establishes an open database connection to the specified MonetDB
}

\note{
  Not much to note here.
}

\value{
  Returns an object that contains the MonetDB connection object that you can
  then use to pass to RMonetDBClose, RMonetDBQuery, and RMonetDBGetInfo.
}
\examples{\dontrun{
# full parameter specification
conn <- RMonetDBConnect(dbhost='localhost', dbuser='voc', dbpass='voc', dbname='voc')

# Use a YAML encoded parameter file
conn <- RMonetDBConnect("db.yml")
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
