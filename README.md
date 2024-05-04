## BigQuery to Spanner with Apache Spark in Scala

### Why create this repo ? 

* Google Cloud customers need to move data from BigQuery to Spaner
* Some customers choose to use Apache Spark in Scala to do so
* There exists to public documentation on how best to do this

### Demo - part 1 of 4 - environment variables + setup

create some environment variables
```shell
export PROJECT_ID=$(gcloud config list core/project --format="value(core.project)")
export PROJECT_NUM=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
export GEO_REGION="US"
export GCS_BUCKET="gs://${PROJECT_ID}-gcpsparkscala"
export GCS_BUCKET_JARS="${GCS_BUCKET}/jars"
export BQ_DATASET="gcpsparkscala_demo_dataset"
export BQ_TABLE="demo"
export SPANNER_INSTANCE="gcpsparkscala-demo-instance"
export SPANNER_DB="gcpsparkscala-demo--db"
export SPANNER_TABLE="demo_data"
export CLUSTER_NAME="gcpsparkscala-demo-cluster"
export APP_JAR_NAME="gcpsparkscala.jar"

```
enable some apis
```shell
gcloud services enable dataproc.googleapis.com
```

### Demo - part 2 of 4 - BigQuery table (source)

make a dataset to house the table

```shell
bq --location=${GEO_REGION} mk \
--dataset \
${PROJECT_ID}:${BQ_DATASET}

```
make a table with schema to represent different [GoogleSQL data types](https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types)

```shell
bq mk \
 --table \
 --expiration 3600 \
 --description "This is a demo table for replication to spanner" \
 ${BQ_DATASET}.${BQ_TABLE} \
 id:INT64,measure1:FLOAT64,measure2:NUMERIC,dim1:BOOL,dim2:STRING
```

create some fake data 

```shell
bq query \
--append_table \
--use_legacy_sql=false \
--destination_table ${BQ_DATASET}.${BQ_TABLE} \
'SELECT
  CAST(2 AS INT64) AS id,
  CAST(6.28 AS FLOAT64) AS measure1,
  CAST(600 AS NUMERIC) AS measure2,
  FALSE AS dim1,
  "blabel" AS dim2'
```

### Demo - part 3 of 4 - Spanner table (sink)

create a spanner instance

```shell
gcloud spanner instances create ${SPANNER_INSTANCE} \
  --project=${PROJECT_ID}  \
  --config=regional-us-central1 \
  --description="Demo replication from BigQuery" \
  --nodes=1
```

create a database within the spanner instance (with dialect GoogleSQL)

```shell
gcloud spanner databases create ${SPANNER_DB} \
  --instance=${SPANNER_INSTANCE}
```

create a table in our Spanner DB, with schema matching BigQuery table
Spanner DDL uses GoogleSQL [data types](https://cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language#data_types)


| column   | BigQuery Type | Spanner Type |
|----------|---------------|--------------|
| id       | INT64         | INT64        |
| measure1 | FLOAT64       | FLOAT64      |
 | measure2 | NUMERIC       | NUMERIC      |
| dim1     | BOOL          | BOOL         |
| dim2     | STRING        | STRING(MAX)  |


```shell
gcloud spanner databases ddl update ${SPANNER_DB} \
--instance=${SPANNER_INSTANCE} \
--ddl='CREATE TABLE demo_data ( id INT64, measure1 FLOAT64, measure2 NUMERIC, dim1 BOOL, dim2 STRING(MAX) ) PRIMARY KEY (id)'
```

create some fake data 

```shell
gcloud spanner rows insert \
  --instance=${SPANNER_INSTANCE} \
  --database=${SPANNER_DB} \
  --table=${SPANNER_TABLE} \
  --data=id=1,measure1=3.14,measure2=300,dim1=TRUE,dim2="label"
```

### Demo - part 4 of 4 - Run Scala Spark Job on Dataproc

create a dataproc cluster

```shell
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region us-central1 \
  --no-address \
  --master-machine-type n2-standard-4 \
  --master-boot-disk-type pd-balanced \
  --master-boot-disk-size 500 \
  --num-workers 2 \
  --worker-machine-type n2-standard-4 \
  --worker-boot-disk-type pd-balanced \
  --worker-boot-disk-size 500 \
  --image-version 2.2-debian12 \
  --project ${PROJECT_ID}
```

create a bucket to hold JARs
```shell
gcloud storage buckets create ${GCS_BUCKET} \
  --project=${PROJECT_ID} \
  --location=${GEO_REGION} \
  --uniform-bucket-level-access
```

Upload required JARs to Google Cloud Storage bucket

 * google-cloud-spanner-jdbc-2.17.1-single-jar-with-dependencies.jar

launch Scala Apache Spark job on Dataproc cluster

```shell
gcloud dataproc jobs submit spark --cluster ${CLUSTER_NAME} \
    --region=us-central1 \
    --jar=${GCS_BUCKET_JARS}/${APP_JAR_NAME} \
    --jars=${GCS_BUCKET_JARS}/google-cloud-spanner-jdbc-2.17.1-single-jar-with-dependencies.jar \
    -- ${PROJECT_ID} ${BQ_DATASET} ${BQ_TABLE} ${SPANNER_INSTANCE} ${SPANNER_DB} ${SPANNER_TABLE}
```

### notes - How types map across BigQuery, Spark & Spanner

This spark job uses the BigQuery connector which maps [data types](https://github.com/GoogleCloudDataproc/spark-bigquery-connector?tab=readme-ov-file#data-types)

| column   | BigQuery Type | SparkSQL Type | Spanner Type |
|----------|---------------|---------------|--------------|
| id       | INT64         | LongType      | INT64        |
| measure1 | FLOAT64       | DoubleType    | FLOAT64      |
| measure2 | NUMERIC       | DecimalType   | NUMERIC      |
| dim1     | BOOL          | BooleanType   | BOOL         |
| dim2     | STRING        | StringType    | STRING(MAX)  |

### Notes - getting dev environment to match dataproc image

Need to match dev environment with environment created by dataproc image

| attribute          | Dataproc                                                                                        | Local Dev                        |
|--------------------|-------------------------------------------------------------------------------------------------|----------------------------------|
| Dataproc image     | [2.2-debian12](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.2) | n/a                              |
| Apache Spark       | 3.5.0                                                                                           | n/a                              |
| BigQuery connector | 0.34.0                                                                                          | n/a                              |
| GCS connector      | 3.0.0                                                                                           | n/a                              |
| Java               | 11                                                                                              | zulu-11 (java version "11.0.20") |
| Scala              | 2.12.18                                                                                         | 2.12.18                          |
| IDE                | n/a                                                                                             | IntelliJ IDEA (2022.3.3)         |
| build system       | n/a                                                                                             | sbt                              |
| sbt                | n/a                                                                                             | 1.9.9                            |






