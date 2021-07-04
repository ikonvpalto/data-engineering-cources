FROM postgres:12

RUN apt-get update \
&& apt-get install -y wget bzip2

COPY init.sh /docker-entrypoint-initdb.d/init.sh

WORKDIR /omdb
COPY ./omdb-postgresql-master .

WORKDIR /pagila
COPY ./pagila-master .

WORKDIR /

RUN chmod +x /usr/local/bin//docker-entrypoint.sh
RUN chmod +x /docker-entrypoint-initdb.d/init.sh
RUN chmod 777 /omdb
RUN chmod 777 /pagila

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["postgres"]
