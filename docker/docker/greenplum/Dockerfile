FROM ubuntu:18.04

ENV GP_DATA_DIR=/opt/greenplum-db-6.8.1/data
ENV GP_MASTER_HOST=gp_master
ENV GP_MASTER_PORT=5433
ENV GP_USER=gpadmin

ENV MASTER_DATA_DIRECTORY=/opt/greenplum-db-6.8.1/data/master/gpsne-1/
ENV MASTER_PORT=5433

COPY provision /tmp/provision

RUN addgroup gpadmin \
  && useradd -p $(openssl passwd -1 secret) -m -g gpadmin gpadmin \
  && usermod -aG sudo gpadmin \
  && mkdir -p /home/gpadmin \
  && chown -R gpadmin:gpadmin /home/gpadmin

RUN apt-get update \
    && apt install -y lzma software-properties-common locales openssh-client openssh-server iputils-ping \
    && apt-add-repository -y ppa:greenplum/db \
    && apt-get update \
    && apt-get -y install greenplum-db=6.8* \
    && sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen \
    && mkdir -p $GP_DATA_DIR/master \
    && mkdir -p $GP_DATA_DIR/data1 \
    && mkdir -p $GP_DATA_DIR/data2 \
    && chown -R gpadmin:gpadmin /tmp/provision \
    && chown -R gpadmin:gpadmin /opt/greenplum-db-6.8.1/

RUN service ssh start \
    && su - gpadmin -c 'mkdir -p /home/gpadmin/.ssh' \
    && ssh-keygen -t rsa -f /home/gpadmin/.ssh/id_rsa -q -N "" \
    && echo $(cat /home/gpadmin/.ssh/id_rsa.pub) >> /home/gpadmin/.ssh/authorized_keys \
    && chown -R gpadmin:gpadmin /home/gpadmin/.ssh \
    && su - gpadmin -c 'ssh-keyscan localhost | tee -a /home/gpadmin/.ssh/known_hosts' \
    && su - gpadmin -c '. /opt/greenplum-db-6.8.1/greenplum_path.sh;gpssh-exkeys -f /tmp/provision/hostlist_singlenode' \
    && su - gpadmin -l -c '. /opt/greenplum-db-6.8.1/greenplum_path.sh;gpinitsystem -a -c /tmp/provision/gpinitsystem_singlenode;exit 0' \
    && echo "export MASTER_DATA_DIRECTORY=/opt/greenplum-db-6.8.1/data/master/gpsne-1/" >> /home/gpadmin/.profile \
    && echo "export MASTER_PORT=5433" >> /home/gpadmin/.profile \
    && echo "host     all         all          0.0.0.0/0         trust" >> $GP_DATA_DIR/master/gpsne-1/pg_hba.conf \
    && echo "host     all         all          0.0.0.0/0         trust" >> $GP_DATA_DIR/data1/gpsne0/pg_hba.conf \
    && echo "host     all         all          0.0.0.0/0         trust" >> $GP_DATA_DIR/data2/gpsne1/pg_hba.conf \
    && su - gpadmin -c ". /opt/greenplum-db-6.8.1/greenplum_path.sh;yes|gpstop"

COPY entrypoint.sh /

EXPOSE 5433

ENTRYPOINT /entrypoint.sh