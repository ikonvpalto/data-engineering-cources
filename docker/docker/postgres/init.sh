#!/bin/bash
cd /omdb
./download && ./import

cd /pagila

psql -U postgres -v ON_ERROR_STOP=1 <<EOF
SET ROLE postgres;
CREATE DATABASE pagila;
EOF

psql -U postgres -v ON_ERROR_STOP=1 -d pagila -f pagila-schema.sql \
&& psql -U postgres -v ON_ERROR_STOP=1 -d pagila -f pagila-data.sql