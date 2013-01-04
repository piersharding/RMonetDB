#!/bin/sh

rm -rf /home/piers/R/x86_64-pc-linux-gnu-library/2.15/RMonetDB
rm src/RMonetDB.o
rm src/RMonetDB.so
rm *.Rout
rm configure

autoconf

R CMD INSTALL --build --preclean --clean .

