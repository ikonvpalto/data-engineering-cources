FROM apache/airflow:1.10.15-python3.8

COPY entrypoint.sh /

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint.sh"]