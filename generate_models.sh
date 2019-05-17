#!/bin/sh
set -e
cat db.sql | psql --host=127.0.0.1 --port=5432 -U root test
sqlboiler --no-hooks psql
