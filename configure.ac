# original by Friedrich Leisch, much changed by BDR

AC_INIT([RMonetDB], 0.0.1, [piers@ompka.net])

dnl Select an optional include path, from a configure option
dnl or from an environment variable.
AC_ARG_WITH([mapi-include],
            AC_HELP_STRING([--with-mapi-include=INCLUDE_PATH],
                           [the location of MonetDB client mapi header files]),
            [mapi_include_path=$withval])
RMonetDB_CPPFLAGS="-I."
if test [ -n "$mapi_include_path" ] ; then
   RMonetDB_CPPFLAGS="-I. -I${mapi_include_path}"
else
  if test [ -n "${MAPI_INCLUDE}" ] ; then
     RMonetDB_CPPFLAGS="-I. -I${MAPI_INCLUDE}"
  fi
fi

dnl ditto for a library path
AC_ARG_WITH([mapi-lib],
            AC_HELP_STRING([--with-mapi-lib=LIB_PATH],
                           [the location of MonetDB client mapi libraries]),
            [mapi_lib_path=$withval])
if test [ -n "$mapi_lib_path" ] ; then
   LIBS="-L${mapi_lib_path} ${LIBS}"
else 
  if test [ -n "${MAPI_LIBS}" ] ; then
     LIBS="-L${MAPI_LIBS} ${LIBS}"
  fi
fi

dnl Now find the compiler and compiler flags to use
: ${R_HOME=`R RHOME`}
if test -z "${R_HOME}"; then
  echo "could not determine R_HOME"
  exit 1
fi
CC=`"${R_HOME}/bin/R" CMD config CC`
CPP=`"${R_HOME}/bin/R" CMD config CPP`
CFLAGS=`"${R_HOME}/bin/R" CMD config CFLAGS`
RMonetDB_EXT_CPPFLAGS=`"${R_HOME}/bin/R" CMD config --cppflags`
CPPFLAGS=`"${R_HOME}/bin/R" CMD config CPPFLAGS`
EXTCPPFLAGS=`"${R_HOME}/bin/R" CMD config --cppflags`

PKG_LIBS=`pkg-config monetdb-mapi --libs`
PKG_CPPFLAGS=`pkg-config monetdb-mapi --cflags`

CPPFLAGS="${CPPFLAGS} ${PKG_CPPFLAGS} ${RMonetDB_CPPFLAGS}"

dnl Check the headers can be found
AC_CHECK_HEADERS(mapi.h)
if test "${ac_cv_header_monetdb_h}" = no; then
   AC_MSG_ERROR("MonetDB MAPI header mapi.h not found")
fi

LIBS="${LIBS} ${PKG_LIBS} -lmapi "
AC_TRY_LINK(
   [
    #include <mapi.h>
   ],[
   Mapi mid;
   mapi_get_mapi_version(mid);
   ],[
   AC_MSG_RESULT(yes! we have libmapi)
   ],[
       AC_MSG_ERROR([libmapi is not found!])
   ]
)

dnl for unixODBC header
AC_CHECK_SIZEOF(long, 4)

RMonetDB_CPPFLAGS="${RMonetDB_CPPFLAGS} ${RMonetDB_EXT_CPPFLAGS} "
echo "HERE: ${RMonetDB_CPPFLAGS}"

dnl substitute RMonetDB_CPPFLAGS and LIBS
AC_SUBST(RMonetDB_CPPFLAGS)
AC_SUBST(CPPFLAGS)
AC_SUBST(LIBS)
AC_CONFIG_HEADERS([src/config.h])

AC_SUBST(PKG_CPPFLAGS)
AC_SUBST(PKG_LIBS)

dnl and do subsitution in the src/Makevars.in and src/config.h
AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
