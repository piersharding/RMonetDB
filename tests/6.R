test.write_frame_to_table <- function()
{
    conn <- RMonetDBConnect("db.yml")
    tab = RMonetDBQuery(conn, "SELECT * FROM voc.voyages LIMIT 250")
    #str(tab)
    checkEquals(250, length(tab$trip))
    res = dbwrite(conn, tab, "tmp_voyages", drop=TRUE, chunk=45)
    res = RMonetDBCommit(conn)
    tab = RMonetDBQuery(conn, "SELECT * FROM voc.tmp_voyages LIMIT 250")
    #str(tab)
    checkEquals(250, length(tab$trip))
    res = RMonetDBExecute(conn, "DROP TABLE voc.tmp_voyages")
    #print(res)
    checkEquals(0, res)

    checkTrue(RMonetDBClose(conn))
}
