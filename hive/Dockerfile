FROM ubuntu:latest
ARG HIVE_BIN_VERSION="3.1.3"
ARG HADOOP_BIN_VERSION="3.3.5"
ENV HIVE_BIN_VERSION=${HIVE_BIN_VERSION}
ENV HADOOP_BIN_VERSION=${HADOOP_BIN_VERSION}

RUN apt-get update && apt-get install -y openjdk-8-jdk curl
RUN mkdir /opt/app && mkdir /opt/app/work

COPY hive/ /tmp/hive/

RUN /tmp/hive/setup.sh
COPY hive-authz/build/libs/hive-authz.jar /opt/app/apache-hive-${HIVE_BIN_VERSION}-bin/lib/hive-authz.jar

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME /opt/app/hadoop-${HADOOP_BIN_VERSION}
ENV HIVE_HOME /opt/app/apache-hive-${HIVE_BIN_VERSION}-bin
ENV PATH $HADOOP_HOME/bin:$HIVE_HOME/bin:$JAVA_HOME/bin:$PATH

WORKDIR /opt/app/work
COPY hive/run.sh /opt/app/work/hive-start.sh
CMD ["./hive-start.sh"]

