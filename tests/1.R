test.connecting <- function()
{
    conn <- RMonetDBConnect("db.yml")
    checkTrue(typeof(attr(conn, 'handle_ptr')) == 'externalptr')
    checkTrue(!is.null(attr(conn, 'handle_ptr')))
    info <- RMonetDBGetInfo(conn)
    #print(info)
    checkEquals(info[['host']], "localhost")
    checkEquals(info[['uri']], "mapi:monetdb://localhost:50000/voc")
    checkEquals(info[['lang']], "sql")
    checkEquals(info[['user']], "voc")
    checkEquals(info[['dbname']], "voc")
    checkTrue(RMonetDBClose(conn))

    conn <- RMonetDBConnect(dbhost='localhost', dbuser='voc', dbpass='voc', dbname='voc')

    checkTrue(typeof(attr(conn, 'handle_ptr')) == 'externalptr')
    checkTrue(!is.null(attr(conn, 'handle_ptr')))
    info <- RMonetDBGetInfo(conn)
    #str(info)
    checkEquals(info[['host']], "localhost")
    checkEquals(info[['uri']], "mapi:monetdb://localhost:50000/voc")
    checkEquals(info[['lang']], "sql")
    checkEquals(info[['user']], "voc")
    checkEquals(info[['dbname']], "voc")
    checkTrue(RMonetDBClose(conn))
}
           
#test.deactivation <- function()
#{
# DEACTIVATED('Deactivating this test function')
#}
