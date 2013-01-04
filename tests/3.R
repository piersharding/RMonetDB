test.query_table_noparms <- function()
{
    conn <- RMonetDBConnect("db.yml")
    res = RMonetDBQuery(conn, "SELECT * FROM voc.craftsmen LIMIT 3")
    #print(res)
    #print(res$number[1])
    checkTrue(length(res$trip) == 3)
    checkEquals(1376, res$number[1])
    checkTrue(RMonetDBClose(conn))
}
