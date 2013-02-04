\name{RMonetDBListFields}
\alias{RMonetDBListFields}
\title{List the fields of a table}
\description{
  List the fields of a MonetDB table
}
\usage{
RMonetDBListFields(con, tablename, schema="")
}%"
\arguments{
  \item{con}{an Open MonetDB Connection handle}
  \item{tablename}{The table name to get fields for}
  \item{schema}{optional - specify a schema if necessary}
}
\details{

  con <- RMonetDBConnect("db.yml")
  res <- RMonetDBListFields(con, "craftsmen")
  print(res)
  RMonetDBClose(con)

}

\note{
  Not much to note here.
}

\value{
  Returns a data.frame representing the fields and characteristics of a table
}
\examples{\dontrun{
# List fields
RMonetDBListFields(con, "craftsmen", schema="voc")
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
