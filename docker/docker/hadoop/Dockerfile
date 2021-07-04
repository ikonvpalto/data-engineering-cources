FROM adoptopenjdk:8u282-b08-jre-openj9-0.24.0-focal

ENV HADOOP_INSTALL=/opt/hadoop
ENV PATH=$PATH:$HADOOP_INSTALL/bin
ENV PATH=$PATH:$HADOOP_INSTALL/sbin
ENV HADOOP_MAPRED_HOME=$HADOOP_INSTALL
ENV HADOOP_COMMON_HOME=$HADOOP_INSTALL
ENV HADOOP_HDFS_HOME=$HADOOP_INSTALL
ENV YARN_HOME=$HADOOP_INSTALL
ENV HADOOP_HOME=$HADOOP_INSTALL
ENV HADOOP_DATA_DIR=$HADOOP_HOME/data
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop/

ARG HADOOP_MASTER_HOST=localhost

RUN apt-get update \
    && apt-get -y install wget tar openssh-client openssh-server \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY install /install
RUN ls -lsa /install

RUN (test -f /install/hadoop-2.10.1.tar.gz \
        || wget https://apache.paket.ua/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz -P /install) \
  && tar xzf /install/hadoop-2.10.1.tar.gz -C /opt \
  && mv /opt/hadoop-2.10.1/ /opt/hadoop/ \
  && rm /install/hadoop-2.10.1.tar.gz

RUN service ssh start \
    && ssh-keygen -t rsa -f ~/.ssh/id_rsa -q -N "" \
    && echo $(cat ~/.ssh/id_rsa.pub) >> ~/.ssh/authorized_keys \
    && echo $HADOOP_MASTER_HOST > $HADOOP_HOME/etc/hadoop/slaves \
    && ssh-keyscan $HADOOP_MASTER_HOST >> ~/.ssh/known_hosts \
    && ssh-keyscan 0.0.0.0 >> ~/.ssh/known_hosts

COPY provision $HADOOP_HOME/etc/hadoop

RUN mkdir -p $HADOOP_DATA_DIR/hdfs/ \
    && hdfs namenode -format

# Install Spark
ENV SPARK_HOME=/opt/spark
# SPARK_DIST_CLASSPATH=$(hadoop classpath)
ENV SPARK_DIST_CLASSPATH=/opt/hadoop/etc/hadoop/:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*:/opt/hadoop/share/hadoop/mapreduce/lib/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/contrib/capacity-scheduler/*.jar
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native
ENV SPARK_LOCAL_IP=0.0.0.0
ENV PYSPARK_PYTHON=/usr/bin/python
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN (test -f /install/spark-3.1.1-bin-without-hadoop.tgz \
     || wget https://apache.paket.ua/spark/spark-3.1.1/spark-3.1.1-bin-without-hadoop.tgz -P /install) \
  && tar xzf /install/spark-3.1.1-bin-without-hadoop.tgz -C /opt \
  && mv /opt/spark-3.1.1-bin-without-hadoop/ /opt/spark/ \
  && rm /install/spark-3.1.1-bin-without-hadoop.tgz

ENTRYPOINT /bin/bash
