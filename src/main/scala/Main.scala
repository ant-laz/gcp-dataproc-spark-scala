package org.tonyz.com

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("gcp-spark-scala")
      .getOrCreate()

    val df = spark.read
      .option("header",value=true)
      .csv("gs://python-lab-329118-scala-spark/AAPL.csv")

    df.show()

  }
}