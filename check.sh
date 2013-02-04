#!/bin/sh

rm RMonetDB*.gz *.Rout; rm -rf *.Rcheck; clear; R CMD build .
FLE=`ls RMonetDB_*.tar.gz`
echo "Eyeball the contents for wrong files:"
tar -tzvf $FLE

echo ""
echo "checking: $FLE"
R CMD check --install=fake --as-cran $FLE
