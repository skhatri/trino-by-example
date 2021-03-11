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
curl -O trino https://repo1.maven.org/maven2/io/trino/trino-cli/353/trino-cli-353-executable.jar
chmod +x trino
```

### Certificates

Generate keystore and truststore
```
keytool -genkeypair -alias trino -keyalg RSA -keystore certs/keystore.jks \
-dname "CN=coordinator, OU=datalake, O=dataco, L=Sydney, ST=NSW, C=AU" \
-ext san=dns:coordinator,dns:coordinator.presto,dns:coordinator.presto.svc,dns:coordinator.presto.svc.cluster.local,dns:coordinator-headless,dns:coordinator-headless.presto,dns:coordinator-headless.presto.svc,dns:coordinator-headless.presto.svc.cluster.local,dns:localhost,ip:127.0.0.1,ip:192.168.64.5,ip:192.168.64.6 \
-storepass password

keytool -exportcert -file certs/trino.cer -alias trino -keystore certs/keystore.jks -storepass password

keytool -import -v -trustcacerts -alias trino_trust -file certs/trino.cer -keystore certs/truststore.jks -storepass password -keypass password -noprompt

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


### Create Table in Hive with S3
I used by google fit app data for this

BUCKET_NAME=my-dataset

aws s3 cp hive/sampledata.csv s3://${BUCKET_NAME}/data/fit/load_date=2021-03-06/

Next, login to hive and create the activity table. Register a new partition also
```
make hive-cli

#set bucket name as variable
set hivevar:bucket_name=<BUCKET_NAME>;
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
```
kubectl apply -f kubernetes/manifest/coordinator
kubectl apply -f kubernetes/manifest/worker
kubectl apply -f kubernetes/manifest/trino-client
```

#### Undeploy
```
kubectl delete -f kubernetes/manifest/coordinator
kubectl delete -f kubernetes/manifest/worker
kubectl delete -f kubernetes/manifest/trino-client
```
