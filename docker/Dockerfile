FROM adoptopenjdk:8u282-b08-jre-openj9-0.24.0-focal

RUN apt-get update \
    && apt-get -y install python3.8 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.8 1 \
    && update-alternatives --set python /usr/bin/python3.8 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1 \
    && update-alternatives --set python3 /usr/bin/python3.8

RUN apt-get -y install python3-pip \
    && python -m pip install --upgrade --force-reinstall pip

RUN apt-key adv --fetch-keys https://apt.postgresql.org/pub/repos/apt/ACCC4CF8.asc \
    && echo "deb http://apt.postgresql.org/pub/repos/apt bionic-pgdg main" >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get -y install libpq-dev=13.* \
    && apt-get -y install python3.8-dev \
    && apt-get -y install wget

RUN python -m pip install requests \
            pandas \
            pyyaml \
            pyspark \
            psycopg2-binary \
            psycopg2 \
            apache-airflow \
            hdfs

ENV SPARK_HOME=/usr/local/lib/python3.8/dist-packages/pyspark/
ENV PYSPARK_PYTHON=/usr/bin/python3.8

WORKDIR /app