test.read_schema <- function()
{
    conn <- RMonetDBConnect("db.yml")
    res = RMonetDBListTables(conn)
    #print(res)
    checkTrue(length(res$name) >= 10)
    res = RMonetDBListFields(conn, "voyages", schema="voc")
    checkEquals(22, length(res$column))
    #print(res)
    checkTrue(RMonetDBClose(conn))
}
