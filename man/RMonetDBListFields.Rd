\name{RMonetDBListFields}
\alias{RMonetDBListFields}
\title{List the fields of a table}
\description{
  List the fields of a MonetDB table
}
\usage{
RMonetDBListFields(conn, "table name")
}%"
\arguments{
  \item{conn}{an Open MonetDB Connection handle}
  \item{table name}{The table name to get fields for}
  \item{schema}{optional - specify a schema if necessary}
}
\details{

  conn <- RMonetDBConnect("db.yml")
  res <- RMonetDBListFields(conn, "craftsmen")
  print(res)
  RMonetDBClose(conn)

}

\note{
  Not much to note here.
}

\value{
  Returns a data.frame representing the fields and characteristics of a table
}
\examples{\dontrun{
# List fields
RMonetDBListFields(conn, "craftsmen", schema="voc")
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
