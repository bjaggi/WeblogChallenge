package com.eva.app


import org.apache.log4j._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, TimestampType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.rdd.RDD


object TTMStreamingAnalytics {
  //Not required on Mac, Windows Only
  System.setProperty("hadoop.home.dir", "C:\\Softwares\\WinUtils");
  /**
    * Main method for windows deveopment, use shell script for spark-submit
    */
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors ( For Development Only )
    //Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
   /* val spark = SparkSession
      .builder
      .master("local")
      .appName("PayTM-Analytics-APP")
      .config("spark.hadoop.hive.cli.print.header", "true")
      .config("spark.driver.allowMultipleContexts","true")
      .config("header", "true")
      .getOrCreate()*/

   // val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val ssc = new StreamingContext("local[*]", "Paytm-StreamingAnalytics", Seconds(7))
    //val linesDStream = ssc.fileStream[LongWritable, Text, TextInputFormat]("C:\\Projects\\SparkScalaBoilerplate\\SampleInputData").map{ case (x, y) => (x.toString, y.toString) }
    val linesDStream = ssc
      .textFileStream("C:\\Projects\\SparkScalaBoilerplate\\SampleInputData")

    linesDStream.foreachRDD { lineRdd =>
      val spark = SparkSessionSingleton.getInstance(lineRdd.sparkContext.getConf)
      import spark.implicits._
      val clickStreamDS = lineRdd.map(line => {
        val parts = line.split(" ")
        println(parts)
        ClickStream(parts(0), parts(2), parts(5), parts(12))
      }).toDS()


      //clickStreamDS.foreach(println(_))

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


    }


    /* linesDStream.foreachRDD { lineRdd =>
        lineRdd.foreach( line=> {
          val parts = line.split(" ")

          val clickStream:ClickStream =  ClickStream(parts(0), parts(2), parts(5), parts(12))
          println(clickStream)
          clickStream
        })
        }
*/
   /* import org.apache.spark.streaming.{Seconds, StreamingContext, Time}


    // Convert RDDs of the words DStream to DataFrame and run SQL query
    linesDStream.foreachRDD { (rdd: RDD[String], time: Time) =>
      // Get the singleton instance of SparkSession
      //val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(line=> {
        val parts = line.split(" ")

        val clickStream:ClickStream = new ClickStream(parts(0), parts(2), parts(5), parts(12))
        println(clickStream)
        clickStream
      })*/






      ssc.start()
    ssc.awaitTermination()
  }

//case class ClickStream(timeStamp:String, clientIpPort : String, backendProcessingTime : String , url : String )
  /**
    *
    * @param line
    * @return
    */
  def mapper(line:String): ClickStream = {
    println("input line "+line)
    val parts = line.split(" ")

    val clickStream:ClickStream = new ClickStream(parts(0), parts(2), parts(5), parts(12))
    println(clickStream)
    return clickStream
  }

  /**
    *
    * @param timeStamp
    * @param clientIpPort
    * @param backendProcessingTime
    * @param url
    */
  case class ClickStream(timeStamp:String, clientIpPort : String, backendProcessingTime : String , url : String )

  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
