#!/bin/sh

R CMD INSTALL --preclean .
R CMD INSTALL --clean .

rm src/RMonetDB.i*
rm src/RMonetDB.o
rm src/RMonetDB.so
rm rfc*.trc
rm *.Rout
rm RMonetDB_*.tar.gz
rm -rf autom4te.cache

