#!/usr/bin/env bash
set -e -o pipefail

query="select * from hive.fitness.activity limit 1"
user="user2"
next_uri=$(curl -s -k --cacert ./certs/trino-proxy/bundle.crt \
--header "Authorization: Basic $(echo -n admin:password|base64)" \
--header "X-Trino-User: ${user}" --data "${query}" \
https://trino-proxy:8453/v1/statement/|jq -r '.nextUri')
while true;
do
    curl -s -o out.json -k --cacert ./certs/trino-proxy/bundle.crt \
    --header "Authorization: Basic $(echo -n admin:password|base64)" \
    --header "X-Trino-User: ${user}" \
    ${next_uri}
    cat out.json
    data_val=$(cat out.json|jq -r '.data')
    if [[ "${data_val}" != "null" ]];
    then
      echo "-----DATA------"
      echo ${data_val}
      echo "---------------"
      break;
    fi;
    next_uri=$(cat out.json|jq -r '.nextUri')
    if [[ "${next_uri}" == "null" ]];
    then
        break;
    fi;
    sleep 1;
done;
