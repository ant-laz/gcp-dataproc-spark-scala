## Scala Spark patterns on Google Cloud Dataproc

This repository contains patterns for the following common tasks on GCP:

* Google Cloud Storage read / write
* Google BigQuery read / write
* Google Cloud Spanner read / write

### Set up

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