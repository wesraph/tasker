#!/bin/sh
set -e
export PGPASSWORD=root
rm models -rf
cat db.sql | psql --host=127.0.0.1 --port=5433 -U root test
sqlboiler --no-hooks psql
