#!/usr/bin/env bash

# Init DB on first start
test -f /opt/airflow/db.lock \
  || (echo "DB Init" \
      && airflow db upgrade \
      && touch /opt/airflow/db.lock)

/entrypoint "$@"