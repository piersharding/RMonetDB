test.execute_query_check_rows <- function()
{
    conn <- RMonetDBConnect("db.yml")
    res = RMonetDBExecute(conn, "SELECT * FROM voc.craftsmen LIMIT 3")
    #print(res)
    checkTrue(as.numeric(res) == 3)
    checkTrue(RMonetDBClose(conn))
}
