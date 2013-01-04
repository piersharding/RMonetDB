/*
 *     Copyright (c) 2012 - and onwards Piers Harding.
 *         All rights reserved.
 *
 *         */

/* RMonetDB low level interface
 *
 */
//#include <config.h>

#include <R.h>
#include <Rdefines.h>
#include <R_ext/PrtUtil.h>
#include <R_ext/Rdynload.h>


#define LST_EL(x,i) VECTOR_ELT((x),(i))
#define CHR_EL(x,i) CHAR_DEREF(STRING_ELT((x),(i)))
#define SET_CHR_EL(x,i,val)  SET_STRING_ELT((x),(i), (val))
#define SET_ROWNAMES(df,n)  setAttrib(df, R_RowNamesSymbol, n)
#define GET_CLASS_NAME(x)   GET_CLASS(x)
#define SET_CLASS_NAME(x,n) SET_CLASS(x, n)
#define LGL_EL(x,i) LOGICAL_POINTER((x))[(i)]


#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <limits.h> /* for INT_MAX */

/* SAP flag for Windows NT or 95 */
#ifdef _WIN32
#  ifndef SAPonNT
#    define SAPonNT
#  endif
#endif

#include <mapi.h>

/*
 * local prototypes & declarations
 */
#define MAX_HANDLES 256
#define MAX_PARAMS 256

/* fake up a definition of bool if it doesnt exist */
#ifndef bool
typedef unsigned char   bool;
#endif

/* create my true and false */
#ifndef false
typedef enum { false, true } mybool;
#endif


typedef struct RMonetDB_CONN_INFO_rec {
    Mapi handle;
} RMonetDB_CONN_INFO, *pRMonetDB_CONN_INFO;


static unsigned int nHandles = 0; /* number of Handles opened in session */
static pRMonetDB_CONN_INFO opened_handles[MAX_HANDLES+1];


static char * query_parameters[MAX_PARAMS+1];


/* create a parameter space and zero it */
static void * make_space(int len){

    char * ptr;
    ptr = malloc( len + 2 );
    if ( ptr == NULL ) {
        return NULL;
    }
    memset(ptr, 0, len + 2);
    return ptr;
}


void makeDataFrame(SEXP data)
{
    S_EVALUATOR SEXP row_names, df_class_name;
    Sint i, n;
    char buf[1024];

    PROTECT(data);
    PROTECT(df_class_name = NEW_CHARACTER((Sint) 1));
    SET_CHR_EL(df_class_name, 0, COPY_TO_USER_STRING("data.frame"));

    /* row.names */
    n = GET_LENGTH(LST_EL(data, 0));    /* length(data[[1]]) */
    PROTECT(row_names = NEW_CHARACTER(n));
    for (i = 0; i < n; i++) {
        (void) sprintf(buf, "%d", i + 1);
        SET_CHR_EL(row_names, i, COPY_TO_USER_STRING(buf));
    }
    SET_ROWNAMES(data, row_names);
    SET_CLASS_NAME(data, df_class_name);
    UNPROTECT(3);
    return;
}


SEXP mkstring(char * str) {

    SEXP ans;

    PROTECT(ans = mkCharLenCE(str, strlen(str), CE_UTF8));
    UNPROTECT(1);
    return ans;
}


SEXP mk_string_len(char * str, int len) {
    SEXP ans;

    PROTECT(ans = mkCharLenCE(str, len, CE_UTF8));
    UNPROTECT(1);
    return ans;
}


SEXP mk_string(char * str) {
    return mk_string_len(str, strlen(str));
}


void rmonetdb_errorcall(Mapi dbh, MapiHdl hdl, char * msg)
{
    if (hdl != NULL) {
        mapi_explain_query(hdl, stderr);
        do {
            if (mapi_result_error(hdl) != NULL) {
                //mapi_explain_result(hdl, stderr);
                //fprintf(stderr, "mapi_result_error\n");
                R_ShowMessage(mapi_result_error(hdl));
            }
        } while (mapi_next_result(hdl) == 1);
        mapi_close_handle(hdl);
        mapi_destroy(dbh);
    } else if (dbh != NULL) {
        //mapi_explain(dbh, stderr);
        R_ShowMessage(mapi_error_str(dbh));
        mapi_destroy(dbh);
        //fprintf(stderr, "command failed\n");
    }
    errorcall(R_NilValue, msg);
}


SEXP RMonetDBConnect(SEXP args)
{
    SEXP names;
    SEXP ptr;
    SEXP ans;
    SEXP elmt;
    S_EVALUATOR SEXP df_class_name;

    MapiHdl hdl = NULL;

    RMonetDB_CONN_INFO *hptr;
    int idx, i;
    const char * dbhost = NULL;
    int dbport = 50000;
    const char * dbuser = NULL;
    const char * dbpass = NULL;
    const char * dblang = "sql";
    const char * dbname = NULL;
    const char * csepk = NULL, * csepv = NULL;

    hptr = malloc(sizeof(RMonetDB_CONN_INFO));
    hptr->handle = NULL;
    idx = length(args);
    names = args;

    if (idx < 1) {
        Rprintf("No connection parameters\n");
        return(R_NilValue);
    }

    for (i = 0; i < idx; i++) {
        elmt = VECTOR_ELT(args, i);
        csepk = translateCharUTF8(STRING_ELT(getAttrib(names, R_NamesSymbol),i));
        csepv = translateCharUTF8(STRING_ELT(elmt, 0));
        if (strcmp(csepk, "dbhost") == 0) {
            dbhost = csepv;
        }
        else if (strcmp(csepk, "dbport") == 0) {
            dbport = atoi(csepv);
        }
        else if (strcmp(csepk, "dbuser") == 0) {
            dbuser = csepv;
        }
        else if (strcmp(csepk, "dbpass") == 0) {
            dbpass = csepv;
        }
        else if (strcmp(csepk, "dblang") == 0) {
            dblang = csepv;
        }
        else if (strcmp(csepk, "dbname") == 0) {
            dbname = csepv;
        }
//        else {
//            fprintf(stderr, "key: %s value: %s \n", csep, csepv);
//        }
    }
    //fprintf(stderr, "host: %s port: %d user: %s pass: %s lang: %s name: %s \n", dbhost, dbport, dbuser, dbpass, dblang, dbname);
    hptr->handle = mapi_connect(dbhost, dbport, dbuser, dbpass, dblang, dbname);
    dbhost = NULL;
    dbuser = NULL;
    dbpass = NULL;
    dblang = NULL;
    dbname = NULL;
    if (mapi_error(hptr->handle)) {
        rmonetdb_errorcall(hptr->handle, hdl, "MonetDB connection open failed");
        return(R_NilValue);
    }

    PROTECT(ans = allocVector(INTSXP, 1));
    INTEGER(ans)[0] = nHandles;
    PROTECT(ptr = R_MakeExternalPtr(hptr, install("RMonetDB_handle"), R_NilValue));
    setAttrib(ans, install("handle_ptr"), ptr);
    if(nHandles <= MAX_HANDLES) opened_handles[nHandles] = hptr;
    PROTECT(df_class_name = NEW_CHARACTER((Sint) 1));
    SET_CHR_EL(df_class_name, 0, COPY_TO_USER_STRING("RMonetDB_Connector"));
    SET_CLASS_NAME(ans, df_class_name);
    UNPROTECT(3);
    return ans;
}


/************************************************
 *
 *        DISCONNECT
 *
 * **********************************************/
SEXP RMonetDBIsConnected(SEXP handle)
{
    SEXP ans;
    PROTECT(ans = NEW_LOGICAL((Sint) 1));
    SEXP ptr = getAttrib(handle, install("handle_ptr"));
    RMonetDB_CONN_INFO *hptr = R_ExternalPtrAddr(ptr);
    if (mapi_is_connected(hptr->handle) != 0 && mapi_ping(hptr->handle) == MOK) {
        LGL_EL(ans, 0) = (Sint) true;
        //return ScalarInteger(1);
    } else {
        LGL_EL(ans, 0) = (Sint) false;
        //return R_NilValue;
    }
    UNPROTECT(1);
    return(ans);
}


SEXP RMonetDBExecute(SEXP handle, SEXP sql, SEXP autocommit, SEXP lastid, SEXP try)
{
    MapiHdl hdl = NULL;
    SEXP ans;

    SEXP exptr = getAttrib(handle, install("handle_ptr"));
    RMonetDB_CONN_INFO *hptr = R_ExternalPtrAddr(exptr);

    if (!isLogical(autocommit) || LOGICAL(autocommit)[0] == TRUE) {
        if (mapi_setAutocommit(hptr->handle, 1) != MOK || mapi_error(hptr->handle)) {
            rmonetdb_errorcall(hptr->handle, hdl, "MonetDB failed to enable autocommit");
            PROTECT(ans = NEW_LOGICAL((Sint) 1));
            LGL_EL(ans, 0) = (Sint) false;
            UNPROTECT(1);
            //return(R_NilValue);
            return(ans);
        }
        //fprintf(stderr, "switched on autocommit\n");
    }
    else {
        if (mapi_setAutocommit(hptr->handle, 0) != MOK || mapi_error(hptr->handle)) {
            rmonetdb_errorcall(hptr->handle, hdl, "MonetDB failed to disable autocommit");
            PROTECT(ans = NEW_LOGICAL((Sint) 1));
            LGL_EL(ans, 0) = (Sint) false;
            UNPROTECT(1);
            //return(R_NilValue);
            return(ans);
        }
        //fprintf(stderr, "NO autocommit set\n");
    }
    //fprintf(stderr, "EXECUTE: %s\n", CHAR(STRING_ELT(sql,0)));

    if (mapi_is_connected(hptr->handle) != 0 && mapi_ping(hptr->handle) == MOK) {
        if ((hdl = mapi_query(hptr->handle, CHAR(STRING_ELT(sql,0)))) == NULL || mapi_error(hptr->handle)) {
            if (!isLogical(try) || LOGICAL(try)[0] == TRUE) {
                PROTECT(ans = NEW_LOGICAL((Sint) 1));
                LGL_EL(ans, 0) = (Sint) false;
                UNPROTECT(1);
                return(ans);
                //return ScalarInteger(0);
            }
            rmonetdb_errorcall(hptr->handle, hdl, "MonetDB query execute failed");
            PROTECT(ans = NEW_LOGICAL((Sint) 1));
            LGL_EL(ans, 0) = (Sint) false;
            UNPROTECT(1);
            return(ans);
            //return(R_NilValue);
        }

        // did we ask for the last affected row id - ie. INSERT?
        if (!isLogical(lastid) || LOGICAL(lastid)[0] == TRUE) {
            return ScalarInteger(mapi_fetch_row(hdl));
        }
        else {
            if (!isLogical(try) || LOGICAL(try)[0] == TRUE) {
                PROTECT(ans = NEW_LOGICAL((Sint) 1));
                LGL_EL(ans, 0) = (Sint) true;
                UNPROTECT(1);
                return(ans);
                //return ScalarInteger(1);
            }
            else {
                return ScalarInteger(mapi_rows_affected(hdl));
            }
        }
    } else {
        PROTECT(ans = NEW_LOGICAL((Sint) 1));
        LGL_EL(ans, 0) = (Sint) false;
        UNPROTECT(1);
        return(ans);
        //return R_NilValue;
    }

}


SEXP RMonetDBClose(SEXP handle)
{
    SEXP ans;

    //fprintf(stderr, "before get attr\n");
    SEXP exptr = getAttrib(handle, install("handle_ptr"));
    //fprintf(stderr, "after get attr\n");
    RMonetDB_CONN_INFO *hptr = R_ExternalPtrAddr(exptr);
//    Rprintf("(CLose)got handle %p\n", hptr);

    // always returns MOK
    mapi_disconnect(hptr->handle);
    mapi_destroy(hptr->handle);
    hptr->handle = NULL;
    free(hptr);
    R_ClearExternalPtr(exptr);
    if (asInteger(handle) <= MAX_HANDLES) opened_handles[asInteger(handle)] = NULL;
    //fprintf(stderr, "before set attr\n");
    PROTECT(handle);
    setAttrib(handle, install("handle_ptr"), R_NilValue);
    UNPROTECT(1);
    //fprintf(stderr, "after set attr\n");
    PROTECT(ans = NEW_LOGICAL((Sint) 1));
    LGL_EL(ans, 0) = (Sint) true;
    UNPROTECT(1);
    return(ans);
    //return Rf_ScalarLogical(1);
}


SEXP RMonetDBGetInfo(SEXP handle)
{
    SEXP exptr = getAttrib(handle, install("handle_ptr"));
    RMonetDB_CONN_INFO *hptr = R_ExternalPtrAddr(exptr);

    SEXP ans, ansnames;
    int i=0;

    // return a list of connection attributes
    PROTECT(ans = allocVector(STRSXP, 7));
    PROTECT(ansnames = allocVector(STRSXP, 7));
    SET_STRING_ELT(ans, i, mkstring(mapi_get_host(hptr->handle)));
    SET_STRING_ELT(ansnames, i++, mkChar("host"));
    SET_STRING_ELT(ans, i, mkstring(mapi_get_uri(hptr->handle)));
    SET_STRING_ELT(ansnames, i++, mkChar("uri"));
    SET_STRING_ELT(ans, i, mkstring(mapi_get_user(hptr->handle)));
    SET_STRING_ELT(ansnames, i++, mkChar("user"));
    SET_STRING_ELT(ans, i, mkstring(mapi_get_lang(hptr->handle)));
    SET_STRING_ELT(ansnames, i++, mkChar("lang"));
    SET_STRING_ELT(ans, i, mkstring(mapi_get_dbname(hptr->handle)));
    SET_STRING_ELT(ansnames, i++, mkChar("dbname"));
    SET_STRING_ELT(ans, i, mkstring(mapi_get_monet_version(hptr->handle)));
    SET_STRING_ELT(ansnames, i++, mkChar("mapi_version"));
    SET_STRING_ELT(ans, i, mkstring(mapi_get_mapi_version(hptr->handle)));
    SET_STRING_ELT(ansnames, i++, mkChar("monet_version"));

    setAttrib(ans, R_NamesSymbol, ansnames);
    UNPROTECT(2);
    return ans;
}


SEXP RMonetDBQuote(SEXP str)
{
    SEXP ans;
    //fprintf(stderr, "Quote: %s\n", CHAR(STRING_ELT(str, 0)));
    PROTECT(ans = allocVector(STRSXP, 1));
    SET_STRING_ELT(ans, 0, mkstring(mapi_quote((char *)CHAR(STRING_ELT(AS_CHARACTER(str),0)), strlen(CHAR(STRING_ELT(AS_CHARACTER(str),0))))));
    UNPROTECT(1);
    return ans;
}


SEXP RMonetDBUnQuote(SEXP str)
{
    SEXP ans;
    //fprintf(stderr, "UnQuote: %s\n", CHAR(STRING_ELT(str, 0)));
    PROTECT(ans = allocVector(STRSXP, 1));
    SET_STRING_ELT(ans, 0, mkstring(mapi_unquote((char *)CHAR(STRING_ELT(str,0)))));
    UNPROTECT(1);
    return ans;
}


SEXP RMonetDBQuery(SEXP handle, SEXP sql, SEXP parameters, SEXP autocommit, SEXP lastid, SEXP page)
{
    SEXP exptr = getAttrib(handle, install("handle_ptr"));
    RMonetDB_CONN_INFO *hptr = R_ExternalPtrAddr(exptr);
    //Rprintf("got handle %p\n", hptr);

    SEXP ans, ansnames, names, name, value;
    bool first;
    int i, j;
    unsigned idx;
    MapiHdl hdl = NULL;
    mapi_int64 rows;
    char *tp;

    if (TYPEOF(sql) != STRSXP) {
        errorcall(R_NilValue, "Query SQL is not a string\n");
        PROTECT(ans = NEW_LOGICAL((Sint) 1));
        LGL_EL(ans, 0) = (Sint) false;
        UNPROTECT(1);
        return(ans);
        //return(R_NilValue);
    }

    if (!isLogical(autocommit) || LOGICAL(autocommit)[0] == FALSE) {
        if (mapi_setAutocommit(hptr->handle, 0) != MOK || mapi_error(hptr->handle)) {
            rmonetdb_errorcall(hptr->handle, hdl, "MonetDB failed to disable autocommit");
            PROTECT(ans = NEW_LOGICAL((Sint) 1));
            LGL_EL(ans, 0) = (Sint) false;
            UNPROTECT(1);
            return(ans);
            //return(R_NilValue);
        }
        //fprintf(stderr, "switch off autocommit\n");
    }
    else {
        if (mapi_setAutocommit(hptr->handle, 1) != MOK || mapi_error(hptr->handle)) {
            rmonetdb_errorcall(hptr->handle, hdl, "MonetDB failed to enable autocommit");
            PROTECT(ans = NEW_LOGICAL((Sint) 1));
            LGL_EL(ans, 0) = (Sint) false;
            UNPROTECT(1);
            return(ans);
            //return(R_NilValue);
        }
        //fprintf(stderr, "switch on autocommit\n");
    }

    idx = length(parameters);
    //fprintf(stderr, "parameters: %d\n", idx);
    //Rprintf("SQL %s\n", CHAR(STRING_ELT(sql, 0)));
    if (idx <= 0) {
        // no parameters - straight query
        if ((hdl = mapi_query(hptr->handle, CHAR(STRING_ELT(sql,0)))) == NULL || mapi_error(hptr->handle)) {
            rmonetdb_errorcall(hptr->handle, hdl, "MonetDB query failed");
            PROTECT(ans = NEW_LOGICAL((Sint) 1));
            LGL_EL(ans, 0) = (Sint) false;
            UNPROTECT(1);
            return(ans);
            //return(R_NilValue);
        }
    }
    else {
        // process query parameters
        // clean the parameter holder
        for (i=0; i<=MAX_PARAMS; i++) {
            query_parameters[i] = NULL;
        }

        // set character parameters
        for (i = 0; i < idx; i++) {
            value = AS_CHARACTER(VECTOR_ELT(parameters, i));
            query_parameters[i] = (char *) CHAR(STRING_ELT(value,0));
            //fprintf(stderr, "parameter value: %s\n", query_parameters[i]);
        }

        // call query with parameters
        if ((hdl = mapi_query_array(hptr->handle, CHAR(STRING_ELT(sql,0)), query_parameters)) == NULL || mapi_error(hptr->handle)) {
            // release parameters
            for (i=0; i<=MAX_PARAMS; i++) {
                query_parameters[i] = NULL;
            }
            rmonetdb_errorcall(hptr->handle, hdl, "MonetDB parameterised query failed");
            PROTECT(ans = NEW_LOGICAL((Sint) 1));
            LGL_EL(ans, 0) = (Sint) false;
            UNPROTECT(1);
            return(ans);
            //return(R_NilValue);
        }
        // cleanup paramtere pointers
        for (i=0; i<=MAX_PARAMS; i++) {
            query_parameters[i] = NULL;
        }
    }

    // did we ask for the last affected row id - ie. INSERT?
    if (!isLogical(lastid) || LOGICAL(lastid)[0] == TRUE) {
        return ScalarInteger(mapi_fetch_row(hdl));
    }

//mapi_get_row_count(hdl);
//mapi_timeout(hptr->handle, the_time);

    rows = mapi_fetch_all_rows(hdl);
    idx = mapi_get_field_count(hdl);
    //fprintf(stderr, "rows received %lld with %d fields\n", rows, idx);


//    fprintf(stderr, "return parameters: %d \n", idx);
    if (rows > 0) {
        PROTECT(ans = allocVector(VECSXP, idx));
        PROTECT(ansnames = allocVector(STRSXP, idx));
        first = true;
        for (i = 0; i < rows; i++) {
            // break out if error
            if (mapi_seek_row(hdl, i, MAPI_SEEK_SET) || mapi_fetch_row(hdl) == 0)
                break;
    
            //fprintf(stderr, "row: %d\n", i);

            // for the first row build out the dataframe structure
            if (first) {
                first = false;
                for (j = 0; j < idx; j++) {
                    // fprintf(stderr, "setting field: %d\n", j);
                    SET_STRING_ELT(ansnames, j, mk_string(mapi_get_name(hdl, j)));
                    tp = mapi_get_type(hdl, j);
                    if (strcmp(tp, "double") == 0 ||
                        strcmp(tp, "dbl") == 0 ||
                        strcmp(tp, "real") == 0 ||
                        strcmp(tp, "curve") == 0 ||
                        strcmp(tp, "geometry") == 0 ||
                        strcmp(tp, "linestring") == 0 ||
                        strcmp(tp, "mbr") == 0 ||
                        strcmp(tp, "multilinestring") == 0 ||
                        strcmp(tp, "point") == 0 ||
                        strcmp(tp, "polygon") == 0 ||
                        strcmp(tp, "surface") == 0) {
                        SET_VECTOR_ELT(ans, j, allocVector(REALSXP, rows));
                    }
                    else if (strcmp(tp, "smallint") == 0 ||
                        strcmp(tp, "int") == 0 ||
                        strcmp(tp, "bigint") == 0) {
                        SET_VECTOR_ELT(ans, j, allocVector(INTSXP, rows));
                    }
                    else {
                        SET_VECTOR_ELT(ans, j, allocVector(STRSXP, rows));
                    }
                }
                setAttrib(ans, R_NamesSymbol, ansnames);
            }
    
            // process each field of each row
            for (j = 0; j < idx; j++) {
                tp = mapi_get_type(hdl, j);
                if (strcmp(tp, "double") == 0 ||
                    strcmp(tp, "dbl") == 0 ||
                    strcmp(tp, "real") == 0 ||
                    strcmp(tp, "curve") == 0 ||
                    strcmp(tp, "geometry") == 0 ||
                    strcmp(tp, "linestring") == 0 ||
                    strcmp(tp, "mbr") == 0 ||
                    strcmp(tp, "multilinestring") == 0 ||
                    strcmp(tp, "point") == 0 ||
                    strcmp(tp, "polygon") == 0 ||
                    strcmp(tp, "surface") == 0) {
                       //strtod
                    //REAL(VECTOR_ELT(ans, j))[i] = REAL(atof(mapi_fetch_field(hdl, j)))[0];
                    //fprintf(stderr, "do REAL: %d\n", j);
                    if (strcmp(mapi_fetch_field(hdl, j), "null") == 0) {
                        REAL(VECTOR_ELT(ans, j))[i] = atof(mapi_fetch_field(hdl, j));
                    }
                    else {
                        REAL(VECTOR_ELT(ans, j))[i] = 0.0;
                    }
                }
                else if (strcmp(tp, "smallint") == 0 ||
                    strcmp(tp, "int") == 0 ||
                    strcmp(tp, "bigint") == 0) {
                    //fprintf(stderr, "do INTEGER: %d\n", j);
                    //INTEGER(VECTOR_ELT(ans, j))[i] = INTEGER(atoi(mapi_fetch_field(hdl, j)))[0];
                    //if (strcmp(mapi_fetch_field(hdl, j), "null") == 0) {
                    if (mapi_fetch_field(hdl, j) == NULL) {
                        INTEGER(VECTOR_ELT(ans, j))[i] = 0;
                    }
                    else {
                        INTEGER(VECTOR_ELT(ans, j))[i] = atoi(mapi_fetch_field(hdl, j));
                    }
                }
                else {
                    //fprintf(stderr, "do OTHER: %d\n", j);
                    if (mapi_fetch_field(hdl, j) == NULL) {
                        SET_STRING_ELT(VECTOR_ELT(ans, j), i, mk_string(""));
                    }
                    else {
                        SET_STRING_ELT(VECTOR_ELT(ans, j), i, mk_string(mapi_fetch_field(hdl, j)));
                    }
                }
                //printf("%s=%s ", mapi_get_name(hdl, j), mapi_fetch_field(hdl, j));
                //mapi_get_type(MapiHdl hdl, j);
            }
        }
        makeDataFrame(ans);
        UNPROTECT(1);
    }
    else {
        // empty table - empty value
        PROTECT(ans = allocVector(VECSXP, 0));
    }
    UNPROTECT(1);

    // check errors on retrieving results
    if (mapi_error(hptr->handle)) {
        rmonetdb_errorcall(hptr->handle, hdl, "MonetDB query retrieve results failed");
        // return(R_NilValue);
    }

    // check errors on closing the query statement handle
    if (mapi_close_handle(hdl) != MOK) {
        rmonetdb_errorcall(hptr->handle, hdl, "MonetDB query close statement handle failed");
        // return(R_NilValue);
    }
    return ans;
}


/* called from .onUnload */
SEXP RMonetDBTerm(void)
{
    return R_NilValue;
}


static const R_CallMethodDef CallEntries[] = {
    {"RMonetDBConnect", (DL_FUNC) &RMonetDBConnect, 1},
    {"RMonetDBIsConnected", (DL_FUNC) &RMonetDBIsConnected, 1},
    {"RMonetDBQuery", (DL_FUNC) &RMonetDBQuery, 6},
    {"RMonetDBExecute", (DL_FUNC) &RMonetDBExecute, 5},
    {"RMonetDBClose", (DL_FUNC) &RMonetDBClose, 1},
    {"RMonetDBGetInfo", (DL_FUNC) &RMonetDBGetInfo, 1},
    {"RMonetDBQuote", (DL_FUNC) &RMonetDBQuote, 1},
    {"RMonetDBUnQuote", (DL_FUNC) &RMonetDBUnQuote, 1},
//    {"RMonetDBTerm", (DL_FUNC) &RMonetDBTerm, 0},
    {NULL, NULL, 0}
};


void R_init_RMonetDB(DllInfo *dll)
{
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}


