FROM trinodb/trino:418


RUN mkdir -p /usr/lib/trino/plugin/ext
COPY trino-ext-authz/build/libs/trino-ext-authz.jar /usr/lib/trino/plugin/ext/trino-ext-authz.jar
COPY trino-ext-authz/build/ext/*.jar /usr/lib/trino/plugin/ext

COPY hive-authz/build/libs/hive-authz.jar /usr/lib/trino/plugin/hive/hive-authz.jar
