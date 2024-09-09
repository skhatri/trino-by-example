#!/usr/bin/env bash

if [[ -z ${HIVE_BIN_VERSION} ]] || [[ -z $HADOOP_BIN_VERSION ]]; then
  echo hadoop and hive bin version required
  exit 1;
fi;

if [[ ! -f /tmp/hive/apache-hive-${HIVE_BIN_VERSION}-bin.tar.gz ]];
then
  curl -o /tmp/hive/apache-hive-${HIVE_BIN_VERSION}-bin.tar.gz https://downloads.apache.org/hive/hive-${HIVE_BIN_VERSION}/apache-hive-${HIVE_BIN_VERSION}-bin.tar.gz
fi;
if [[ ! -f /tmp/hive/hadoop-${HADOOP_BIN_VERSION}.tar.gz ]];
then
  curl -o /tmp/hive/hadoop-${HADOOP_BIN_VERSION}.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_BIN_VERSION}/hadoop-${HADOOP_BIN_VERSION}.tar.gz
fi;

tar zxf /tmp/hive/apache-hive-${HIVE_BIN_VERSION}-bin.tar.gz -C /opt/app && \
    tar zxf /tmp/hive/hadoop-${HADOOP_BIN_VERSION}.tar.gz -C /opt/app && \
    rm /opt/app/apache-hive-${HIVE_BIN_VERSION}-bin/lib/guava-19.0.jar && \
    cp /opt/app/hadoop-${HADOOP_BIN_VERSION}/share/hadoop/hdfs/lib/guava-27.0-jre.jar /opt/app/apache-hive-${HIVE_BIN_VERSION}-bin/lib/ && \
    cp /opt/app/hadoop-${HADOOP_BIN_VERSION}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_BIN_VERSION}.jar /opt/app/apache-hive-${HIVE_BIN_VERSION}-bin/lib/ && \
    cp /opt/app/hadoop-${HADOOP_BIN_VERSION}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.316.jar /opt/app/apache-hive-${HIVE_BIN_VERSION}-bin/lib/ && \
    rm /tmp/hive/*.gz

