\name{RMonetDBQuery}
\alias{RMonetDBQuery}
\alias{dbquery}
\title{Query MonetDB using SQL}
\description{
  Run arbitrary SQL queries against the MonetDB and return the results as a data.frame
}
\usage{
RMonetDBQuery(con, sql, parameters=list(), autocommit=FALSE, lastid=FALSE, page=FALSE)
}%"
\arguments{
  \item{con}{an Open MonetDB Connection handle}
  \item{sql}{Arbitrary SQL to execute}
  \item{parameters}{list of string values substituted into the ? placeholders}
  \item{autocommit}{optional - boolean flag TRUE to auto commit}
  \item{lastid}{optional - boolean flag TRUE to return the last row ID affected - good for INSERT statements}
  \item{page}{optional - boolean flag TRUE to page the results - does nothing at the moment :-)}
}
\details{
  \preformatted{
  con <- RMonetDBConnect("db.yml")
  res <- RMonetDBQuery(con,  "SELECT * FROM voc.craftsmen WHERE trip > ? LIMIT ?", list(3,15))
  print(res)

  RMonetDBClose(con)
  }

}

\note{
  Not much to note here.
}

\value{
  Returns a data.frame of the table contents selected
}
\examples{\dontrun{
# read the VOC craftsmen table with parameter substitutions
RMonetDBQuery(con,  "SELECT * FROM voc.craftsmen WHERE trip > ? LIMIT ?", list(3,15))
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
