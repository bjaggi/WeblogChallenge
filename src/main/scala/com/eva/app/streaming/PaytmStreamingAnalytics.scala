package com.eva.app.streaming

import com.eva.app.utils.{ClickStream, SparkSessionSingleton}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataTypes, TimestampType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PaytmStreamingAnalytics {
  //Not required on Mac, Windows Only
  //System.setProperty("hadoop.home.dir", "C:\\Softwares\\WinUtils");
  /**
    * Main method for windows deveopment, use shell script for spark-submit
    */
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors ( For Development Only )
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ssc = new StreamingContext("local[*]", "Paytm-Streaming-Analytics", Seconds(15))
    val clickStreamLinesDStream = ssc
      .textFileStream("data")
    //.textFileStream("C:\\Projects\\SparkScalaBoilerplate\\SampleInputData")

    clickStreamLinesDStream.foreachRDD { lineRdd =>
      val spark = SparkSessionSingleton.getInstance(lineRdd.sparkContext.getConf)
      import spark.implicits._
      val clickStreamDS = lineRdd.map(line => {
        val parts = line.split(" ")
        ClickStream(parts(0), parts(2), parts(5), parts(12))
      }).toDS()


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

      clickStreamWithSessionTimeDS.show(5, false)

      //Goal3 :Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
      println("\n\n\n")
      println("  Goal-3 -a  : Determine unique URL visits per session ")
      val clickStreamGroupedByIpAddrURL = clickStreamDS
        .groupBy("clientIpPort")
        .agg(collect_set("url"))
        .toDF("clientIpPort", "_2").withColumnRenamed("_2", "UNIQUE_URLS_VISITED")
        .cache()
        .show(5, false) // print to console


      //Goal4 : Find the most engaged users, ie the IPs with the longest session times
      println("\n\n\n")
      println("  Goal-4 : Most engaged users, IPs with the longest session times")
      clickStreamWithSessionTimeDS.show(5, false)
      println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
      println(" Starting Next Window ! ")
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
