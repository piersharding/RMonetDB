test.query_table_with_parms <- function()
{
    conn <- RMonetDBConnect("db.yml")
    res = RMonetDBQuery(conn, "SELECT * FROM voc.craftsmen WHERE trip > ? LIMIT ?", list(3,15))
    checkEquals(15, length(res$trip))
    checkTrue(RMonetDBClose(conn))
}
