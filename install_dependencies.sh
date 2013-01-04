#!/bin/sh

sudo apt-add-repository http://dev.monetdb.org/downloads/deb/ quantal monetdb

sudo wget --output-document=- http://dev.monetdb.org/downloads/MonetDB-GPG-KEY | sudo apt-key add -

sudo apt-get update

sudo apt-get install libmonetdb-stream-dev libmonetdb-client-dev

R --no-save < install_dependencies.R

