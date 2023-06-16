FROM ubuntu:latest
RUN apt-get update && echo y | apt-get install openjdk-8-jdk
RUN mkdir /opt/app && mkdir /opt/app/work

#https://downloads.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
COPY hive/apache-hive-3.1.2-bin.tar.gz /opt/app/
#https://archive.apache.org/dist/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz
COPY hive/hadoop-3.2.2.tar.gz /opt/app/
RUN tar zxf /opt/app/apache-hive-3.1.2-bin.tar.gz -C /opt/app && \
    tar zxf /opt/app/hadoop-3.2.2.tar.gz -C /opt/app && \
    rm /opt/app/apache-hive-3.1.2-bin/lib/guava-19.0.jar && \
    cp /opt/app/hadoop-3.2.2/share/hadoop/hdfs/lib/guava-27.0-jre.jar /opt/app/apache-hive-3.1.2-bin/lib/ && \
    cp /opt/app/hadoop-3.2.2/share/hadoop/tools/lib/hadoop-aws-3.2.2.jar /opt/app/apache-hive-3.1.2-bin/lib/ && \
    cp /opt/app/hadoop-3.2.2/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.563.jar /opt/app/apache-hive-3.1.2-bin/lib/ && \
    rm /opt/app/*.gz

COPY hive-authz/build/libs/hive-authz.jar /opt/app/apache-hive-3.1.2-bin/lib/hive-authz.jar

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME /opt/app/hadoop-3.2.2
ENV HIVE_HOME /opt/app/apache-hive-3.1.2-bin
ENV PATH $HADOOP_HOME/bin:$HIVE_HOME/bin:$JAVA_HOME/bin:$PATH

WORKDIR /opt/app/work
COPY hive/run.sh /opt/app/work/hive-start.sh
CMD ["./hive-start.sh"]

