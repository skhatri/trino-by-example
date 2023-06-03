#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -eo pipefail

if [[ -d /opt/superset/certs ]];
then
  echo updating certificate chain
  mkdir -p /usr/share/ca-certificates/superset
  cp /opt/superset/certs/*.crt /usr/share/ca-certificates/superset/
  for f in $(ls /usr/share/ca-certificates/superset);
  do 
    echo superset/$f | tee -a /etc/ca-certificates.conf
  done;
  update-ca-certificates
fi;

superset db upgrade && superset init

superset fab create-admin --username admin --firstname Admin --lastname Admin --email dev@github.com --password admin && \
    superset fab create-user --role Alpha --username user1 --firstname User --lastname One --email user1@github.com --password password && \
    superset fab create-user --role Alpha --username user2 --firstname User --lastname Two --email user2@github.com --password password && \
    superset fab create-user --role Alpha --username user3 --firstname User --lastname Three --email user3@github.com --password password && \
    superset fab create-user --role Alpha --username skhatri --firstname S --lastname Khatri --email skhatri@github.com --password password

if [ "${#}" -ne 0 ]; then
    exec "${@}"
else
    gunicorn \
        --bind  "0.0.0.0:${SUPERSET_PORT}" \
        --access-logfile '-' \
        --error-logfile '-' \
        --workers 1 \
        --worker-class gthread \
        --threads 20 \
        --timeout ${GUNICORN_TIMEOUT:-60} \
        --limit-request-line 0 \
        --limit-request-field_size 0 \
        "${FLASK_APP}"
fi
