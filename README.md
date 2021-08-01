## Overview
This is a recipe to create a Presto/Trino cluster with hive.

### Tasks
- Download Client
- Certificates
- Configure Environment/Secrets
- Test Trino Connectivity
- Create Table in Hive with S3
- Queries using Hive
- Queries using Presto
- Running in Kubernetes


### Download client
```
curl -O trino https://repo1.maven.org/maven2/io/trino/trino-cli/359/trino-cli-359-executable.jar
chmod +x trino
```

### Certificates

Generate keystore and truststore
```
keytool -genkeypair -alias trino -keyalg RSA -keystore certs/keystore.jks \
-dname "CN=coordinator, OU=datalake, O=dataco, L=Sydney, ST=NSW, C=AU" \
-ext san=dns:coordinator,dns:coordinator.presto,dns:coordinator.presto.svc,dns:coordinator.presto.svc.cluster.local,dns:coordinator-headless,dns:coordinator-headless.presto,dns:coordinator-headless.presto.svc,dns:coordinator-headless.presto.svc.cluster.local,dns:localhost,dns:trino-proxy,ip:127.0.0.1,ip:192.168.64.5,ip:192.168.64.6 \
-storepass password

keytool -exportcert -file certs/trino.cer -alias trino -keystore certs/keystore.jks -storepass password

keytool -import -v -trustcacerts -alias trino_trust -file certs/trino.cer -keystore certs/truststore.jks -storepass password -keypass password -noprompt


keytool -keystore certs/keystore.jks -exportcert -alias trino -storepass password| openssl x509 -inform der -text

keytool -importkeystore -srckeystore certs/keystore.jks -destkeystore certs/trino.p12 -srcstoretype jks -deststoretype pkcs12 -storepass password 

openssl pkcs12 -in certs/trino.p12 -out certs/trino.pem

openssl x509 -in certs/trino.cer -inform DER -out certs/trino.crt

```

### Configure Environment/Secrets
Let's start presto coordinator, presto worker, hive, postgres using docker-compose.
Before starting it, please perform the following

```
cp .env.template .env
```
and put the secrets in .env file

Run ```./env.sh``` to substitute S3 and Postgres password in hive-site.xml


docker-compose up can be run using ```make up```
Check the progress using ```make ps``` and once all containers are up, navigate to http://localhost:8443/ui/ with credentials admin/password to view the presto UI.

### Test Trino Connectivity
Run ```make presto-cli``` with password as "password" to login to presto using the presto executable jar.
Here issue command like ```show catalogs``` to see all available catalogs for query. 

The same query can be run through rest URI

```
curl -k https://coordinator:8443/v1/statement/ \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-User: admin" \
--header "X-Trino-Schema: sf1" \
--header "X-Trino-Source: somesource" \
--header "X-Trino-Time-Zone: UTC" \
--header "X-Trino-Catalog: tpch" \
--header "User-Agent: presto-cli" \
--data "select * from nation"

```

### Create Table in Hive with S3
I used by google fit app data for this

BUCKET_NAME=my-dataset

aws s3 cp hive/sampledata.csv s3://${BUCKET_NAME}/data/fit/load_date=2021-03-06/

Next, login to hive and create the activity table. Register a new partition also
```
make hive-cli

#set bucket name as variable
set hivevar:bucket_name=skhatri-dataset;
CREATE DATABASE fitness;
use fitness;

CREATE EXTERNAL TABLE activity (
    activity_date date,
    average_weight string,
    max_weight string,
    min_weight string,
    calories float,
    heart_points float,
    heart_minutes float,
    low_latitude float,
    low_longitude float,
    high_latitude float,
    high_longitude float,
    step_count int,
    distance float,
    average_speed float,
    max_speed float,
    min_speed float,
    move_minutes_count int,
    cycling_duration float,
    inactive_duration float,
    walking_duration float,
    running_duration float
) PARTITIONED BY (load_date date)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3a://${hivevar:bucket_name}/data/fit'
TBLPROPERTIES ("skip.header.line.count"="1");

ALTER TABLE activity
ADD PARTITION (load_date='2021-03-06') LOCATION 's3a://${hivevar:bucket_name}/data/fit/load_date=2021-03-06';
```

Create Delta Lake Table
```
CREATE DATABASE finance;


DROP TABLE IF EXISTS finance.activity;

#Using spark delta generated manifest
CREATE EXTERNAL TABLE finance.activity(
    account string,
    txn_id string,
    merchant string,
    category string,
    last_updated timestamp,
    deleted boolean,
    txn_date date,
    amount float
) partitioned by (version date)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/opt/data/output/activity/_symlink_format_manifest/';

MSCK REPAIR TABLE finance.activity;

DROP TABLE IF EXISTS finance.activity_snapshot;

CREATE EXTERNAL TABLE finance.activity_snapshot(
    account string,
    txn_id string,
    merchant string,
    category string,
    last_updated timestamp,
    deleted boolean,
    txn_date date,
    amount float
) partitioned by (version date)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/opt/data/output/activity/_delta_manifest/';

MSCK REPAIR TABLE finance.activity_snapshot;

```

### Queries using Hive
Just using hive, we can do the following queries as well.

#### How much have I walked since I installed the fit app?
```
use fitness;
select sum(step_count) as steps, sum(distance)/1000 as distance_in_kms from activity;
```

#### Monthly activity summary
```
select year(activity_date), month(activity_date), sum(heart_points) as heart_pt, sum(step_count) as steps, sum(distance)/1000 as km
from activity
group by year(activity_date), month(activity_date);
```

### Queries using Presto

```
make presto

show catalogs;
show schemas in hive;
show tables in hive.fitness;

select sum(step_count) as steps, sum(distance)/1000 as distance_in_kms from hive.fitness.activity;

use hive.fitness;
select year(activity_date) as year, month(activity_date) as month, sum(heart_points) as heart_pt, sum(step_count) as steps, sum(distance)/1000 as km
from activity
group by year(activity_date), month(activity_date);
```

### Query Delta Data with Presto

View https://github.com/skhatri/spark-delta-by-example for relevant spark delta code

#### Show available snapshots

```
select version, count(*) 
from hive.finance.activity_snapshot
group by version
order by version asc;
```

#### Show latest
```
select count(*)
from hive.finance.activity;
```

#### Find total number of activities by account

```
select account, count(*) from hive.finance.activity
group by account;
```

#### As of a specific snapshot
As of version 3, what was transaction id ```txn10``` labelled as?

```
select * from hive.finance.activity_snapshot
where version=cast('2021-03-02' as date) and txn_id='txn10';

select * from hive.finance.activity_snapshot
where version=cast('2021-03-09' as date) and txn_id='txn10';

```

#### Latest Label
What is the latest label of transaction id ```txn10``` and when was it last updated?
```
select * from hive.finance.activity
where txn_id='txn10';

```

#### Label Change over time
Acc5 bought something from Apple Store Sydney on 2021-03-05, how did the label for this transaction change over time?

```
#latest
select account, txn_date, category, last_updated, txn_id
from hive.finance.activity
where account = 'acc5' and txn_date=cast('2021-03-05' as date) and merchant='Apple Store Sydney';

#versioned
select account, txn_date, category, last_updated, txn_id
from hive.finance.activity_snapshot
where version=cast('2021-03-05' as date) and account = 'acc5' and txn_date=cast('2021-03-05' as date) and merchant='Apple Store Sydney';
```

### Access Control

Create users

```
htpasswd -C 10 -B -c security/passwords/password.db user1
htpasswd -C 10 -B security/passwords/password.db user2
htpasswd -C 10 -B security/passwords/password.db user3
htpasswd -C 10 -B security/passwords/password.db skhatri
htpasswd -C 10 -B security/passwords/password.db admin
```


|User|Access|
|---|---|
|user1|finance.\*,tpch,information_schema|
|user2|fitness.activity,tpch,information_schema|
|user3|finance.activity,tpch,information_schema|
|skhatri|finance.\*, fitness.\*,tpch,information_schema|
|admin|*|

refer to security/rules/rules.json for configuration

#### Querying as Users

Setup a database in Superset using the following URL and connect params

```
trino://admin:password@coordinator:8443/hive
```

Connect Params like this is for testing only and it is recommended to use cacert once basic connectivity is verified
```
{
    "connect_args": {
        "verify": false
    }
}
```

c2toYXRyaTpwYXNzd29yZAo=

curl --cacert ./certs/trino.crt \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-Source: trino-python-client" \
--header "X-Trino-Time-Zone: UTC" \
--header "User-Agent: presto-cli" \
--header "X-Trino-User: user1" \
--header "X-Trino-Catalog: hive" \
--header "X-Trino-Schema: fitness" \
--data "select * from hive.fitness.activity" \
https://coordinator:8443/v1/statement/


Impersonation configuration for superset queries
```json 
,
  "impersonation": [
    {
      "original_user": "(admin|dev)",
      "new_user": ".*",
      "allow": true
    },
    {
      "original_user": "skhatri",
      "new_user": "admin",
      "allow": false
    },
    {
      "original_user": "skhatri",
      "new_user": ".*",
      "allow": true
    }
  ]
  }
```

curl -k --header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
https://coordinator:8443/v1/statement/executing/20210725_132727_00074_tbc5s/yfd855b4a5f7abd77513138384d23060005d9402c/0

### Running with Envoy

Create trino-proxy certificate
```
./ca.sh trino-proxy
```

curl --cacert ./certs/trino-proxy/bundle.crt \
--header "Authorization: Basic $(echo -n admin:password|base64)" \
--header "X-Trino-Source: trino-python-client" \
--header "X-Trino-Time-Zone: UTC" \
--header "User-Agent: presto-cli" \
--header "X-Trino-User: user2" \
--header "X-Trino-Catalog: hive" \
--header "X-Trino-Schema: finance" \
--header "X-Trino-Session: " \
--header "X-Request-Id: 8ce0920c-a8c5-4587-949c-b268dfc80030" \
--data "select * from hive.fitness.activity" \
https://trino-proxy:8453/v1/statement/

curl --cacert ./certs/trino-proxy/bundle.crt \
--header "Authorization: Basic $(echo -n admin:password|base64)" \
--header "X-Trino-User: user2" \
"https://trino-proxy:8453/v1/statement/executing/20210731_125801_00118_hhque/ydd6edd05eec3597d2c76c03e5943f46d4956aeec/0"



### Running in Kubernetes
check manifests at kubernetes/manifests/coordinator and kubernetes/manifests/worker to apply them.

#### updating certificates
```
base64 certs/keystore.jks|pbcopy
base64 certs/truststore.jks|pbcopy
base64 certs/trino.cer|pbcopy
```
Paste the base64 text in coordinator-cert-secret.yaml and worker-cert-secret.yaml

#### Deploy to Kubernetes

Deploy postgres

```
./deploy.sh postgres
```

Initialise it once to create hive database

```
kubectl exec -it postgres-0 -- psql -U postgres
# create database hive;
# \q
```

Deploy other apps
```
./deploy.sh hive
./deploy.sh coordinator
./deploy.sh worker
./deploy.sh trino-client
./deploy.sh superset
```

Trino Connection can be configured in superset

```
url:
presto://admin:password@coordinator-headless:8443/catalog/hive

extra params:
{
    "metadata_params": {},
    "engine_params": {
        "connect_args": {
            "protocol": "https",
             "requests_kwargs": {
                 "verify": "/opt/superset/certs/trino.pem"
             }
       }
    },
    "metadata_cache_timeout": {},
    "schemas_allowed_for_csv_upload": []
}
```




Scale Down resources if there are resource issues
```
helm del trino-client
kubectl scale sts worker --replicas=1
```

