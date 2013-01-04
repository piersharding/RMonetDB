\name{RMonetDBLoadDataFrame}
\alias{RMonetDBLoadDataFrame}
\alias{dbwrite}
\title{Load a data.frame into a MonetDB table}
\description{
  Load a data.frame object into a MonetDB table with automatic schema detection
}
\usage{
RMonetDBLoadDataFrame(conn, frame, "table name")
}%"
\arguments{
  \item{conn}{an Open MonetDB Connection handle}
  \item{frame}{The data.frame to be loaded into a MonetDB table}
  \item{table name}{The table name to load the data.frame into}
  \item{drop}{optional - flag to indicate to drop the table if it exists before recreating on the load}
  \item{tmp.file}{optional - flag to use write.csv and temp file instead of memory for table contents formatting - useful for largedata.frame processing}
  \item{append}{optional - flag to indicate appending dta.frame to an existing table}
}
\details{

  conn <- RMonetDBConnect("db.yml")
  res <- RMonetDBLoadDataFrame(conn, craftsmen, "tmp_craftsmen")
  print(res)
  RMonetDBClose(conn)

}

\note{
  Not much to note here.
}

\value{
  Returns loaded row count - should match the data.frame passed in
}

\examples{\dontrun{
# Close the connection
RMonetDBLoadDataFrame(conn, my.frame, "tmp_tab", drop=TRUE)
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
