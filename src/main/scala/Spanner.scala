object Hello {
  println("Hello, World!")
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
    import com.google.cloud.spanner.{DatabaseClient, Mutation, SpannerOptions}
    import java.util.Collections

    JdbcDialects.registerDialect(
      new JdbcDialect() {
        override def canHandle(url: String): Boolean = url.toLowerCase.startsWith("jdbc:cloudspanner:")

        override def quoteIdentifier(column: String): String = column
      }
    )

    val spark = SparkSession.builder()
      .appName("spark-spanner-demo")
      .config("spark.master", "local")
      .getOrCreate()
    
    import spark.implicits._

    // Load data in from Spanner. See
    // https://github.com/GoogleCloudDataproc/spark-spanner-connector/blob/main/README.md#properties
    // for option information.
    val singersDF =
    (spark.read.format("cloud-spanner")
      .option("projectId", "prabha-poc")
      .option("instanceId", "test-instance")
      .option("databaseId", "example-db")
      .option("enableDataBoost", true)
      .option("table", "Albums")
      .load()
      .cache())
    singersDF.createOrReplaceTempView("test")

    // Load the Singers table.
    val result_spanner = spark.sql("SELECT * FROM test")
    result_spanner.show()
    result_spanner.printSchema()

    // Load data in from Spanner. See
    // https://cloud.google.com/spanner/docs/jdbc-drivers &
    // https://cloud.google.com/spanner/docs/use-oss-jdbc connect via jdbc
    import spark.implicits._

    val jdbcUrl = "jdbc:cloudspanner:/projects/prabha-poc/instances/test-instance/databases/example-db"
    val singersDF_jdbc = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "com.google.cloud.spanner.jdbc.JdbcDriver")
      .option("dbtable", "a")
      .load()
    singersDF_jdbc.createOrReplaceTempView("test")
    val result_jdbc = spark.sql("SELECT * FROM test")
//
    result_jdbc.show()
    result_jdbc.printSchema()

    val data = Seq(
      (1, "Prabha"),
      (2, "Arya")
    )
    val rdd = spark.sparkContext.parallelize(data)
    rdd.foreachPartition { partition =>
      // Establish Spanner connection within each partition
      val options = SpannerOptions.newBuilder().build()
      val spanner = options.getService()
      val databaseClient = spanner.getDatabaseClient(
        com.google.cloud.spanner.DatabaseId.of("prabha-poc", "test-instance", "example-db")
      )

      partition.foreach { case (userId, name) =>
        // Build mutation for each record
        val mutation = Mutation.newInsertOrUpdateBuilder("Users_tab")
          .set("userId").to(userId)
          .set("name").to(name)
          .build()

        // Apply mutation
        databaseClient.write(Collections.singletonList(mutation))
      }
    }

//    Read from BigQuery table
    val df = spark.read.bigquery("prabha-poc.lbg.spanner_out")
    df.show()
    val result: Seq[(Long, Long, String)] = df.as[(Long, Long, String)].collect().toSeq

//    Write to Cloud Spanner using Mutation
    val rdd = spark.sparkContext.parallelize(result)
    rdd.foreachPartition { partition =>
      // Establish Spanner connection within each partition
      val options = SpannerOptions.newBuilder().build()
      val spanner = options.getService()
      val databaseClient = spanner.getDatabaseClient(com.google.cloud.spanner.DatabaseId.of("prabha-poc", "test-instance", "example-db"))
      partition.foreach {  case (singerId, albumId, albumTitle) =>
        // Build mutation for each record
        val mutation = Mutation.newInsertOrUpdateBuilder("bigquery_out")
          .set("SingerId").to(singerId)
          .set("AlbumId").to(albumId)
          .set("AlbumTitle").to(albumTitle)
          .build()
        // Apply mutation
        databaseClient.write(Collections.singletonList(mutation))
      }
    }

//    Saving the data to Spanner, need to have table already created!!
//    result_jdbc.write
//      .format("jdbc")
//      .option("url", jdbcUrl)
//      .option("driver", "com.google.cloud.spanner.jdbc.JdbcDriver")
//      .option("dbtable", "b")
//      .mode("append")
//      .save()


    // Saving the data to Spanner --> didn't work
    //    val writeOptions = Map(
    //      "instanceId" -> "test-instance",
    //      "databaseId" -> "example-db",
    //      "table" -> "gcs_singers"
    //    )
    //
    //    result.write.format("spanner")
    //      .mode("append")
    //      .options(writeOptions).save()


    // Saving the data to BigQuery
    result_spanner.write.format("bigquery")
      .option("writeMethod", "direct")
      .mode("overwrite")
      .save("lbg.b")

    //stop spark creating a SUCCESS file when outputting files to GCS
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    // Saving the data to GCS
    val writer = result_spanner.write
      .mode("Overwrite")
      .format("csv")

    writer.save("gs://test-lbg-cloudera/gcs/spanner_output_file.csv")

  }
}
