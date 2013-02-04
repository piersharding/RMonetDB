\name{RMonetDBConnect}
\alias{RMonetDBConnect}
\title{RMonetDB Open Connections to the database}
\description{
  Open connections to a MonetDB database
}
\usage{
RMonetDBConnect(...)

}%"
\arguments{
  \item{...}{all MonetDB connection parameters for the MAPI client library}
}
\details{
  \code{RMonetDBConnect} establishes an open database connection to the specified MonetDB
  There are two styles of passing the connection parameters:
  - \code{RMonetDBConnect('db.yml')} where the name of A YAML encoded file containing
    MonetDB connection parameters is passed in
  - \code{RMonetDBConnect(dbhost='localhost', dbuser='voc', dbpass='voc', dbname='voc', ...)}
    The individual connection parameters are passed as per the requirements of the MonetDB mapi library
    These parameters are typically:
     * dbhost - the host and socket connection string/uri of MonetDB
     * dbuser - MonetDB user name
     * dbpass - user password
     * dbname - MonetDB name

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
con <- RMonetDBConnect(dbhost='localhost', dbuser='voc', dbpass='voc', dbname='voc')

or

# Use a YAML encoded parameter file
con <- RMonetDBConnect("db.yml")

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
