#!/bin/sh

#export MAPI_INCLUDE=/home/piers/code/sap/nwrfcsdk/include
#export MAPI_LIBS=/home/piers/code/sap/nwrfcsdk/lib

rm -rf /home/piers/R/x86_64-pc-linux-gnu-library/2.15/RMonetDB
rm src/RMonetDB.o
rm src/RMonetDB.so
rm *.Rout
rm configure

autoconf

R CMD INSTALL --build --preclean --clean .
#R CMD INSTALL --build --preclean --clean  --configure-args='--with-mapi-include=/home/piers/code/sap/nwrfcsdk/include --with-mapi-lib=/home/piers/code/sap/nwrfcsdk/lib' .

