#!/usr/bin/env bash

LOCK_FILE=/opt/greenplum-db-6.8.1/data/db.lock

service ssh start
su - gpadmin -c '. /opt/greenplum-db-6.8.1/greenplum_path.sh;gpstart -a'

# Create DB on first start
test -f $LOCK_FILE \
  || (echo "Create DB $GREENPLUM_DB" \
      && su - gpadmin -c ". /opt/greenplum-db-6.8.1/greenplum_path.sh;psql -p 5433 -d template1 -c \"alter user $GREENPLUM_USER password '$GREENPLUM_PASSWORD'\"; createdb $GREENPLUM_DB -p 5433;" \
      && touch $LOCK_FILE)

/bin/bash