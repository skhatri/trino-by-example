## Overview
This is a recipe to create a Presto/Trino cluster with hive.

## Index
- Download Client
- Certificates
- Configure Environment/Secrets
- Test Trino Connectivity
- Setup S3 environment
- Create Table in Hive with S3
- Queries using Hive
- Queries using Trino
- Access Control
- Querying with Superset
- Running with Envoy



### Download client
```shell
curl -s -o trino https://repo1.maven.org/maven2/io/trino/trino-cli/418/trino-cli-418-executable.jar
chmod +x trino
```

### Certificates

Generate keystore and truststore
```shell
keytool -genkeypair -alias trino -keyalg RSA -keystore certs/keystore.jks \
-dname "CN=coordinator, OU=datalake, O=dataco, L=Sydney, ST=NSW, C=AU" \
-ext san=dns:coordinator,dns:coordinator.presto,dns:coordinator.presto.svc,dns:coordinator.presto.svc.cluster.local,dns:coordinator-headless,dns:coordinator-headless.presto,dns:coordinator-headless.presto.svc,dns:coordinator-headless.presto.svc.cluster.local,dns:localhost,dns:trino-proxy,ip:127.0.0.1,ip:192.168.64.5,ip:192.168.64.6 \
-storepass password

keytool -exportcert -file certs/trino.cer -alias trino -keystore certs/keystore.jks -storepass password

keytool -import -v -trustcacerts -alias trino_trust -file certs/trino.cer -keystore certs/truststore.jks -storepass password -keypass password -noprompt


keytool -keystore certs/keystore.jks -exportcert -alias trino -storepass password| openssl x509 -inform der -text

keytool -importkeystore -srckeystore certs/keystore.jks -destkeystore certs/trino.p12 -srcstoretype jks -deststoretype pkcs12 -srcstorepass password -deststorepass password 

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

It is highly advisable that you update your /etc/hosts or equivalent file with the following host entries so we can use
same DNS names inside docker as well as in your cli.

/etc/hosts:

```
127.0.0.1	coordinator worker-1 worker-0 trino-proxy
127.0.0.1	postgres
```

docker-compose up can be run using ```make up```
Check the progress using ```make ps``` and once all containers are up, navigate to https://localhost:8443/ui/ with credentials admin/password to view the presto UI.

### Test Trino Connectivity
Run ```make trino``` with password as "password" to login to trino using the trino executable jar.
Here issue command like ```show catalogs;``` to see all available catalogs for query. 

```sql
show catalogs;
show schemas in tpch;
show tables in tpch.sf1;
select * from tpch.sf1.nation;
```

The same query can be run through rest URI

```shell
curl -k https://coordinator:8443/v1/statement/ \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-User: admin" \
--header "X-Trino-Schema: sf1" \
--header "X-Trino-Source: somesource" \
--header "X-Trino-Time-Zone: UTC" \
--header "X-Trino-Catalog: tpch" \
--header "User-Agent: trino-cli" \
--data "select * from nation"

```

### Setup S3 environment
create a s3 bucket for this project and upload test data to s3 bucket


replace endpoint-url if not running minio at all.
```
while read line; do export $line; done < .env;
AWS_ACCESS_KEY_ID=${STORE_KEY} AWS_SECRET_ACCESS_KEY=${STORE_SECRET} \
aws s3 --endpoint-url http://localhost:9005 mb s3://my-trino-dataset
```
I used by google fit app data for this

```
AWS_ACCESS_KEY_ID=${STORE_KEY} AWS_SECRET_ACCESS_KEY=${STORE_SECRET} \
aws s3 --endpoint-url http://localhost:9005 cp hive/sampledata.csv s3://my-trino-dataset/data/fit/load_date=2021-03-06/
```

also upload sample finance data to another slice in the same bucket

```
AWS_ACCESS_KEY_ID=${STORE_KEY} AWS_SECRET_ACCESS_KEY=${STORE_SECRET} \
aws s3 --endpoint-url http://localhost:9005 cp --recursive hive/output s3://my-trino-dataset/data/finance/output
```

Let's list the data in object storage to verify that the upload is successful
```
AWS_ACCESS_KEY_ID=${STORE_KEY} AWS_SECRET_ACCESS_KEY=${STORE_SECRET} \
aws s3 --endpoint-url http://localhost:9005 ls s3://my-trino-dataset/data --recursive
```

### Create Table in Hive with S3

Next, login to hive and create the activity table. Register a new partition also

```shell
make hive-cli
```

#set bucket name as variable

```hiveql
set hivevar:bucket_name=my-trino-dataset;
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

```hiveql
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
LOCATION 's3a://${hivevar:bucket_name}/data/finance/output/activity/_symlink_format_manifest/';

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
LOCATION 's3a://${hivevar:bucket_name}/data/finance/output/activity/_delta_manifest/';

MSCK REPAIR TABLE finance.activity_snapshot;

```

### Queries using Hive
Just using hive, we can do the following queries as well.

#### How much have I walked since I installed the fit app?
```sql
use fitness;
select sum(step_count) as steps, sum(distance)/1000 as distance_in_kms from activity;
```

#### Monthly activity summary
```sql
select year(activity_date), month(activity_date), sum(heart_points) as heart_pt, sum(step_count) as steps, sum(distance)/1000 as km
from activity
group by year(activity_date), month(activity_date);
```

### Queries using Trino

```shell
make trino
```

```sql
show catalogs;
show schemas in hive;
show tables in hive.fitness;

select sum(step_count) as steps, sum(distance)/1000 as distance_in_kms from hive.fitness.activity;

use hive.fitness;
select year(activity_date) as year, month(activity_date) as month, sum(heart_points) as heart_pt, sum(step_count) as steps, sum(distance)/1000 as km
from activity
group by year(activity_date), month(activity_date);
```

### Query Delta Data with Trino

View https://github.com/skhatri/spark-delta-by-example for relevant spark delta code

#### Show available snapshots

```sql
select version, count(*) 
from hive.finance.activity_snapshot
group by version
order by version asc;
```

|version   | _col1|
|----------|------|
|2021-03-02|    15|
|2021-03-03|    30|
|2021-03-05|    45|
|2021-03-08|    47|
|2021-03-09|    50|

There are 5 versions of the activity dataset for 5 date partitions. The last version has 50 records.


#### Show latest

```sql
select count(*)
from hive.finance.activity;
```

|_col0|
|-----|
|   50|

The activity table also contains the same number of records as the latest version. These records would be merged.

#### Find total number of activities by account

```sql
select account, count(*) as activity_count from hive.finance.activity
group by account;
```

#### As of a specific snapshot
As of version 2021-03-02, what was transaction id ```txn10``` labelled as?

```sql
select * from hive.finance.activity_snapshot
where version=cast('2021-03-02' as date) and txn_id='txn10';
```

|account | txn_id |     merchant     | category  |      last_updated       | deleted |  txn_date  | amount |  version|
|---------|--------|------------------|-----------|-------------------------|---------|------------|--------|-----------|
|acc4    | txn10  | Prouds Jewellery | Jewellery | 2021-03-01 13:00:00.000 | false   | 2021-03-02 |  189.0 | 2021-03-02 |

(1 row)

```sql
select * from hive.finance.activity_snapshot
where version=cast('2021-03-09' as date) and txn_id='txn10';

```

|account | txn_id |     merchant     | category |      last_updated       | deleted |  txn_date  | amount |  version |
|---------|--------|------------------|----------|-------------------------|---------|------------|--------|-----------|
|acc4    | txn10  | Prouds Jewellery | Fashion  | 2021-03-07 13:00:00.000 | false   | 2021-03-02 |  189.0 | 2021-03-09 |


#### Latest category
What is the latest category of transaction id ```txn10``` and when was it last updated?

```sql
select * from hive.finance.activity
where txn_id='txn10';

```

|account | txn_id |     merchant     | category |      last_updated       | deleted |  txn_date  | amount |  version |
|---------|--------|------------------|----------|-------------------------|---------|------------|--------|---------|
|acc4    | txn10  | Prouds Jewellery | Fashion  | 2021-03-07 13:00:00.000 | false   | 2021-03-02 |  189.0 | 2021-03-02|


#### Category Change over time
Acc5 bought something from Apple Store Sydney on 2021-03-05, how did the category for this transaction change over time?

```sql
select account, txn_date, category as latest_category, last_updated, txn_id
from hive.finance.activity
where account = 'acc5' and txn_date=cast('2021-03-05' as date) and merchant='Apple Store Sydney';

select account, txn_date, category as versioned_category, last_updated, txn_id
from hive.finance.activity_snapshot
where version=cast('2021-03-05' as date) and account = 'acc5' and txn_date=cast('2021-03-05' as date) and merchant='Apple Store Sydney';
```
It was initially labelled as Hardware and later it got changed to Phone.

### Access Control

Let's create few users whose access will be configured in trino rules file.

```
htpasswd -C 10 -B -c security/passwords/password.db user1
htpasswd -C 10 -B security/passwords/password.db user2
htpasswd -C 10 -B security/passwords/password.db user3
htpasswd -C 10 -B security/passwords/password.db skhatri
htpasswd -C 10 -B security/passwords/password.db admin
```

Refer to security/rules/rules.json for configuration, the following table summaries the ACL by user.


|User|Access|
|---|---|
|user1|finance.\*,tpch,information_schema|
|user2|fitness.activity,tpch,information_schema|
|user3|finance.activity,tpch,information_schema|
|skhatri|finance.\*, fitness.\*,tpch,information_schema|
|admin|*|

Restart coordinator and worker-1 instances:

```shell
docker-compose stop worker-1
docker-compose stop coordinator
docker-compose start coordinator
docker-compose start worker-1
```

#### Querying as Users
Let's login as user1 and explore their privileges.

```shell
./trino --debug --user=user1 --password --truststore-path=./certs/truststore.jks --truststore-password=password --server https://localhost:8443
```

While the ```show catalogs;``` command lists the available catalogs. Listing schema in one of the catalogs like tpch will only show information_schema.
If we try to retrieve customer data from tpch.sf1, it will throw exception:

```sql 
select * from tpch.sf1.customer limit 2;
```
As user1 does not have access to tpch, the query will return error

```
Query 20220507_183600_00046_z589m failed: Access Denied: Cannot select from table tpch.sf1.customer
io.trino.spi.security.AccessDeniedException: Access Denied: Cannot select from table tpch.sf1.customer
```

In rules.json we have the following permission:

```json 
    {
      "user": "user1",
      "catalog": "hive",
      "schema": "finance",
      "table": ".*",
      "privileges": [
        "SELECT"
      ]
    }
```
We have granted user1 the access to query any table in schema finance of the catalog hive. Let's test this out.

```sql

show schemas in hive;
```

|       Schema     |
|------------------|
|finance           |
|information_schema|

```sql 
show tables in hive.finance;
```

So we can see finance schema for hive in the list of schema available here. While you can check the available tables with ```show tables in hive.finance```, 
let's use our prior knowledge of hive finance tables and query activity table.

```sql 
select * from hive.finance.activity limit 2;
```
Yes, we can retrieve the data as user1

|account | txn_id |     merchant      | category |      last_updated       | deleted |  txn_date  | amount |  version|
|--------|--------|-------------------|----------|-------------------------|---------|------------|--------|---------|
|acc2    | txn36  | Kathmandu Miranda | Clothing | 2021-03-04 13:00:00.000 | false   | 2021-03-05 |  69.95 | 2021-03-05|
|acc3    | txn22  | Optus Phone       | Phone    | 2021-03-02 13:00:00.000 | false   | 2021-03-03 |   40.0 | 2021-03-03|

The rules.json can be reloaded periodically using ```security.refresh-period``` when using File system access control. If using this
ACL in Kubernetes environment, it is possible to run a sidecar container along with coordinator which can fetch the file periodically
from git or remote resource. Refresh option is available for access control file as well as the passwords file.

Refer to trino-ext-authz folder for a external authz plugin that is based on default file system access control. 

Possible extension areas can be calling Open Policy Agent server from the external authz plugin to determine or downloading rules json from a remote resource and caching it.
A new yaml DSL can be maintained to keep rules simpler.

### Querying with Superset

Setup a database in Superset using the following URL and connect params


Loging to http://localhost:9000/ with admin/admin

Go to the screen to add a new database http://localhost:9000/databaseview/add and configure the following:

|Field|Value|
|---|---|
|Database|trino|
|SQLAlchemy URI|trino://admin:password@coordinator:8443/hive|
|Impersonate the logged on user|enable it|
|Secure Extra|{"connect_args": {"verify": false}}|
|Security| Paste content of ca.crt and trino.crt|

Click Test Connection to verify the configuration is ok. Once configured, open http://localhost:9000/superset/sqllab
and execute 

```sql 
select * 
from hive.finance.activity
limit 2
```
The query should return some data.

Log out and try to login as user1 with password "password". Select data from hive.finance.activity.

```sql 
select *
from hive.finance.activity 
limit 2;
```
This should result in successful query execution and with some data being displayed. Behind the scenes, trino driver sent impersonation 
header for user1 even though the connection is established with admin user. 

Let's try query that user1 does not have access to run.

```sql 
select * 
from tpch.sf1.nation
limit 2
```

The following error should be displayed when running the above query as user1.

```
Unexpected Error
base error: Access Denied: Cannot select from table tpch.sf1.nation
```


Impersonation configuration for superset queries is also present in security/rules/rules.json

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

#### Query via REST call

Note, you can also run the query via command line like so:

```shell
curl --cacert ./certs/trino.crt \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-Source: trino-python-client" \
--header "X-Trino-Time-Zone: UTC" \
--header "User-Agent: trino-cli" \
--header "X-Trino-User: user1" \
--header "X-Trino-Catalog: hive" \
--header "X-Trino-Schema: fitness" \
--data "select * from hive.fitness.activity" \
https://coordinator:8443/v1/statement/
```

The query runs asynchronously and result is to be collected by the client by calling url returned as nextUri.

```json
{"id":"20220507_202039_00316_z589m","infoUri":"https://coordinator:8443/ui/query.html?20220507_202039_00316_z589m","nextUri":"https://coordinator:8443/v1/statement/queued/20220507_202039_00316_z589m/y66e5ba5e33aa4a2d883eb1de5b1b34a70d49fa5b/1","stats":{"state":"QUEUED","queued":true,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":0,"elapsedTimeMillis":0,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"warnings":[]}
```

```shell

curl --cacert ./certs/trino.crt \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-Source: trino-python-client" \
--header "X-Trino-Time-Zone: UTC" \
--header "User-Agent: trino-cli" \
--header "X-Trino-User: user1" \
--header "X-Trino-Catalog: hive" \
--header "X-Trino-Schema: fitness" \
https://coordinator:8443/v1/statement/queued/20220507_202039_00316_z589m/y66e5ba5e33aa4a2d883eb1de5b1b34a70d49fa5b/1
```

```json 
{"id":"20220507_202039_00316_z589m","infoUri":"https://coordinator:8443/ui/query.html?20220507_202039_00316_z589m","nextUri":"https://coordinator:8443/v1/statement/queued/20220507_202039_00316_z589m/y2a208f52803793fff6721c73899be69224a8361b/2","stats":{"state":"QUEUED","queued":true,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":3,"elapsedTimeMillis":5,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"warnings":[]}
```

We will keep on calling nextUri 

```shell 
curl --cacert ./certs/trino.crt \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-Source: trino-python-client" \
--header "X-Trino-Time-Zone: UTC" \
--header "User-Agent: trino-cli" \
--header "X-Trino-User: user1" \
--header "X-Trino-Catalog: hive" \
--header "X-Trino-Schema: fitness" \
https://coordinator:8443/v1/statement/queued/20220507_202039_00316_z589m/y2a208f52803793fff6721c73899be69224a8361b/2
```

The response is returned but it has access denied exception. This is valid as we are trying to impersonate "user1" who does not have access to hive.fitness.activity

```json 
{"id":"20220507_202039_00316_z589m","infoUri":"https://coordinator:8443/ui/query.html?20220507_202039_00316_z589m","stats":{"state":"FAILED","queued":false,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":3,"elapsedTimeMillis":46,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"error":{"message":"Access Denied: Cannot select from table hive.fitness.activity","errorCode":4,"errorName":"PERMISSION_DENIED","errorType":"USER_ERROR","failureInfo":{"type":"io.trino.spi.security.AccessDeniedException","message":"Access Denied: Cannot select from table hive.fitness.activity","suppressed":[]}
```

We will now remove the impersonation user and try the query again.

```shell 
curl --cacert ./certs/trino.crt \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-Source: trino-python-client" \
--header "X-Trino-Time-Zone: UTC" \
--header "User-Agent: trino-cli" \
--header "X-Trino-Catalog: hive" \
--header "X-Trino-Schema: fitness" \
--data "select * from hive.fitness.activity limit 1" \
https://coordinator:8443/v1/statement/
```

Clients like superset have the configuration to poll for response. In the background, they would also be calling nextUri just like we are.

```json 
{"id":"20220507_204009_00318_z589m","infoUri":"https://coordinator:8443/ui/query.html?20220507_204009_00318_z589m","nextUri":"https://coordinator:8443/v1/statement/queued/20220507_204009_00318_z589m/yd279ea2a17c75c545a33a7e2ca6ef153fbb8959d/1","stats":{"state":"QUEUED","queued":true,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":0,"elapsedTimeMillis":0,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"warnings":[]}
```

call retrieve what is in nextUri 
```shell 
curl --cacert ./certs/trino.crt \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-Source: trino-python-client" \
--header "X-Trino-Time-Zone: UTC" \
--header "User-Agent: trino-cli" \
--header "X-Trino-Catalog: hive" \
--header "X-Trino-Schema: fitness" \
https://coordinator:8443/v1/statement/queued/20220507_204009_00318_z589m/yd279ea2a17c75c545a33a7e2ca6ef153fbb8959d/1
```

```json 
{"id":"20220507_204009_00318_z589m","infoUri":"https://coordinator:8443/ui/query.html?20220507_204009_00318_z589m","nextUri":"https://coordinator:8443/v1/statement/queued/20220507_204009_00318_z589m/ye6a1823d14841fe8b51fe72c8128097f23cbfd2c/2","stats":{"state":"QUEUED","queued":true,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":8,"elapsedTimeMillis":9,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"warnings":[]}
```

Let's keep on calling nextUri for completeness.
```shell
curl --cacert ./certs/trino.crt \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-Source: trino-python-client" \
--header "X-Trino-Time-Zone: UTC" \
--header "User-Agent: trino-cli" \
--header "X-Trino-Catalog: hive" \
--header "X-Trino-Schema: fitness" \
https://coordinator:8443/v1/statement/queued/20220507_204009_00318_z589m/ye6a1823d14841fe8b51fe72c8128097f23cbfd2c/2
```

```json 
{"id":"20220507_204009_00318_z589m","infoUri":"https://coordinator:8443/ui/query.html?20220507_204009_00318_z589m","nextUri":"https://coordinator:8443/v1/statement/executing/20220507_204009_00318_z589m/y210ffea7ebcb7b54ef598335bf18e2b331427888/0","stats":{"state":"QUEUED","queued":true,"scheduled":false,"nodes":0,"totalSplits":0,"queuedSplits":0,"runningSplits":0,"completedSplits":0,"cpuTimeMillis":0,"wallTimeMillis":0,"queuedTimeMillis":8,"elapsedTimeMillis":21373,"processedRows":0,"processedBytes":0,"physicalInputBytes":0,"peakMemoryBytes":0,"spilledBytes":0},"warnings":[]}
```
```shell

curl --cacert ./certs/trino.crt \
--header "Authorization: Basic YWRtaW46cGFzc3dvcmQ=" \
--header "X-Trino-Source: trino-python-client" \
--header "X-Trino-Time-Zone: UTC" \
--header "User-Agent: trino-cli" \
--header "X-Trino-Catalog: hive" \
--header "X-Trino-Schema: fitness" \
https://coordinator:8443/v1/statement/executing/20220507_204009_00318_z589m/y210ffea7ebcb7b54ef598335bf18e2b331427888/0
```

The final response has the columns and data attributes.

```json
{"id":"20220507_204009_00318_z589m","infoUri":"https://coordinator:8443/ui/query.html?20220507_204009_00318_z589m","partialCancelUri":"https://coordinator:8443/v1/statement/executing/partialCancel/20220507_204009_00318_z589m/1/yc1e5c2e839286131959d53aec4c005e10c352b6b/1","nextUri":"https://coordinator:8443/v1/statement/executing/20220507_204009_00318_z589m/yc1e5c2e839286131959d53aec4c005e10c352b6b/1","columns":[{"name":"activity_date","type":"date","typeSignature":{"rawType":"date","arguments":[]}},{"name":"average_weight","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}},{"name":"max_weight","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}},{"name":"min_weight","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}},{"name":"calories","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"heart_points","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"heart_minutes","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"low_latitude","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"low_longitude","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"high_latitude","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"high_longitude","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"step_count","type":"integer","typeSignature":{"rawType":"integer","arguments":[]}},{"name":"distance","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"average_speed","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"max_speed","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"min_speed","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"move_minutes_count","type":"integer","typeSignature":{"rawType":"integer","arguments":[]}},{"name":"cycling_duration","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"inactive_duration","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"walking_duration","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"running_duration","type":"real","typeSignature":{"rawType":"real","arguments":[]}},{"name":"load_date","type":"date","typeSignature":{"rawType":"date","arguments":[]}}],"data":[["2019-10-03","","","",2909.2954,72.0,63.0,33.9449,-122.476845,37.809143,-118.40078,19363,13745.628,1.3299762,18.032257,0.045891136,222,null,6.4861504E7,7164427.0,6170764.0,"2021-03-06"]],"stats":{"state":"RUNNING","queued":false,"scheduled":true,"nodes":2,"totalSplits":7,"queuedSplits":0,"runningSplits":1,"completedSplits":6,"cpuTimeMillis":32,"wallTimeMillis":143,"queuedTimeMillis":8,"elapsedTimeMillis":39369,"processedRows":521,"processedBytes":96423,"physicalInputBytes":96423,"peakMemoryBytes":638,"spilledBytes":0,"rootStage":{"stageId":"0","state":"RUNNING","done":false,"nodes":1,"totalSplits":5,"queuedSplits":0,"runningSplits":0,"completedSplits":5,"cpuTimeMillis":3,"wallTimeMillis":4,"processedRows":1,"processedBytes":574,"physicalInputBytes":0,"failedTasks":0,"coordinatorOnly":false,"subStages":[{"stageId":"1","state":"PENDING","done":false,"nodes":2,"totalSplits":2,"queuedSplits":0,"runningSplits":1,"completedSplits":1,"cpuTimeMillis":29,"wallTimeMillis":139,"processedRows":521,"processedBytes":96423,"physicalInputBytes":96423,"failedTasks":0,"coordinatorOnly":false,"subStages":[]}]},"progressPercentage":85.71428571428571},"warnings":[]}
```

### Running with Envoy
Although trino has its plugin system for observability and acl, it is also possible to put envoy proxy on top of coordinator.

The benefits of this can be 
- free observability through envoy
- possible ACL or additional rules applied enforced in envoy filter
- zero-downtime trino upgrades
- traffic load shedding based on user, time etc 

Optionally, create trino-proxy certificate which can be configured for trino-proxy envoy instance.

```
./ca.sh trino-proxy
```

The below script uses trino-proxy to queue up the query and retrieves data by polling the response.
```shell
query="select * from hive.fitness.activity limit 1"
user="user2"
next_uri=$(curl -s -o out.json -k --cacert ./certs/trino-proxy/bundle.crt \
--header "Authorization: Basic $(echo -n admin:password|base64)" \
--header "X-Trino-User: ${user}" \
--data "${query}" \
https://trino-proxy:8453/v1/statement/|jq -r '.nextUri')
while true;
do
    curl -s -o out.json -k --cacert ./certs/trino-proxy/bundle.crt \
    --header "Authorization: Basic $(echo -n admin:password|base64)" \
    --header "X-Trino-User: ${user}" \
    ${next_uri}
    cat out.json
    next_uri=$(cat out.json|jq -r '.nextUri')
    if [[ "${next_uri}" == "null" ]];
    then
        break;
    fi;
    sleep 1;
done;
```

