package com.eva.app

import org.apache.spark.sql.types.{DataTypes, TimestampType}
import org.apache.log4j._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import com.eva.app.PaytmUtils.mapper


object PaytmBatchAnalytics {
  //Not required on Mac, Windows Only
  //System.setProperty("hadoop.home.dir", "C:\\Softwares\\WinUtils\\");
  /**
    * Main method for windows deveopment, use shell script for spark-submit
    */
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors ( For Development Only )
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .master("local")
      .appName("PayTM-Analytics-APP")
      .config("spark.hadoop.hive.cli.print.header", "true")
      .config("header", "true")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our ClickStream case class to infer the schema.
    import spark.implicits._
    //val lines = spark.sparkContext.textFile("SampleInputData/2015_07_22_mktplace_shop_web_log_sample.log.gz")
    val lines = spark.sparkContext.textFile("data/sample_click_stream.txt")
    val clickStreamDS = lines.map(mapper).toDS().cache()

    println("Here is our inferred schema:")
    clickStreamDS.printSchema()
    println("\n\n\n")

    // Goal1 : Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session. https://en.wikipedia.org/wiki/Session_(web_analytics)
    val clickStreamGroupedByIpDS = clickStreamDS.groupBy("clientIpPort")
    println(" Goal-1 : Number of Site Visited Per User  ")
    clickStreamGroupedByIpDS
      .count()
      .orderBy($"count".desc)
      .toDF("clientIpPort", "_2")
      .withColumnRenamed("_2", "Page_Hit_Count")
      .show(5, false)


    //Goal2 :Determine the average session time
    //https://aws.amazon.com/blogs/big-data/create-real-time-clickstream-sessions-and-run-analytics-with-amazon-kinesis-data-analytics-aws-glue-and-amazon-athena/
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions._
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val clickStreamWithTimeStmpDS = clickStreamDS
      .withColumn("timeStampTS", unix_timestamp($"timeStamp", "yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType))
      .withColumn("timeStamp_ms", unix_timestamp($"timeStamp", "yyyy-MM-dd'T'HH:mm:ss").cast(DataTypes.LongType))
    clickStreamWithTimeStmpDS.cache()

    println(" Goal-2-a : Average Session Time per unique IP ")
    val clickStreamWithSessionTimeDS = clickStreamWithTimeStmpDS
      .groupBy("clientIpPort")
      .agg(max("timeStamp_ms") - min("timeStamp_ms"))
      .toDF("clientIpPort", "_2").withColumnRenamed("_2", "Session_Time_In_Sec")
      .filter($"Session_Time_In_Sec" > 0)
      .orderBy($"Session_Time_In_Sec".desc)
      .cache()

    clickStreamWithSessionTimeDS.show(5,false)

    println("\n\n\n")
    println(" Goal-2-b : Overall Average Session Time ")
    clickStreamWithSessionTimeDS.agg(
      avg($"Session_Time_In_Sec").as("Avg_Session_Time")
    ).cache().show(5, false) // Print on the console

    clickStreamWithSessionTimeDS
      .coalesce(1)
      .write        // Write to File
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("OutPutProcessedData/goal_2_average_session_time.txt")


    //Goal3 :Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    println("\n\n\n")
    println("  Goal-3 -a  : Determine unique URL visits per session ")
    val clickStreamGroupedByIpAddrURL = clickStreamDS
      .groupBy("clientIpPort")
      .agg(collect_set("url"))
      .toDF("clientIpPort", "_2").withColumnRenamed("_2", "UNIQUE_URLS_VISITED")
      .cache()
      .show(5, false) // print to console


    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    clickStreamDS.createOrReplaceTempView("click_stream")
    val sortedUniqueUrlDF = spark.sql(
      "SELECT clientIpPort , count(distinct url) as distinct_url_count, collect_set(url) as UNIQUE_URLS  " +
        "         FROM click_stream " +
        "         GROUP BY clientIpPort " +
        "  Order By distinct_url_count desc ")

    // Print sample on the Console
    println("\n\n\n")
    println("  Goal-3 -b  : Determine unique URL visits per session, sorted by unique url count ")
    // Commenting this as sorting took a long time on real data and local machine
    val results = sortedUniqueUrlDF.take(5)
    results.foreach(println)
    sortedUniqueUrlDF
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save("OutPutProcessedData/goal_3_unique_visit_per_session.txt")


    //Goal4 : Find the most engaged users, ie the IPs with the longest session times
    println("\n\n\n")
    println("  Goal-4 : Most engaged users, IPs with the longest session times")
    clickStreamWithSessionTimeDS.show(5, false)
    clickStreamWithSessionTimeDS.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("OutPutProcessedData/goal_4_most_active_users.txt")


    // GraceFul Spark Shutdown !
    spark.stop()
  }



}
