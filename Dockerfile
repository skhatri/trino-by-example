FROM trinodb/trino:359

RUN mkdir -p /usr/lib/trino/plugin/ext
COPY trino-ext-authz/build/libs/trino-ext-authz.jar /usr/lib/trino/plugin/ext/trino-ext-authz.jar
COPY trino-ext-authz/build/ext/*.jar /usr/lib/trino/plugin/ext

