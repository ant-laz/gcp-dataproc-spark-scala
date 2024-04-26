package org.tonyz.com

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, desc}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("gcp-spark-scala")
      .getOrCreate()

    //Scala example 1  - Google Cloud Storage read/write
    //https://cloud.google.com/dataproc/docs/tutorials/gcs-connector-spark-tutorial
    //https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage#use_the_connector
    val df = spark.read
      .option("header",value=true)
      .csv("gs://python-lab-329118-scala-spark/AAPL.csv")

    val busyDaysDF = df.select("Date", "Volume")
      .where(col("Volume") > 10000000)
      .orderBy(desc("Volume"))

    //stop spark creating a SUCCESS file when outputting files to GCS
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    val writer = busyDaysDF.write
      .mode("Overwrite")
      .format("csv")

    writer.save("gs://python-lab-329118-scala-spark/AAPL_filtered.csv")

  }
}