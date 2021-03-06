package com.eva.app

import java.sql.{ Timestamp}

import org.apache.spark.sql.types.{DataTypes, DateType, TimestampType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.unix_timestamp

import scala.collection.mutable

object BBCBatchAnalytics {
  //Not required on Mac, Windows Only
  System.setProperty("hadoop.home.dir", "C:\\Softwares\\WinUtils\\");

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors ( For Development Only )
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .master("local")
      .appName("HSBC-Analytics-APP")
      .config("spark.hadoop.hive.cli.print.header", "true")
      .config("header", "true")
      .getOrCreate()


    // Convert our csv file to a DataSet, using our ClickStream case class to infer the schema.
    import spark.implicits._
    val lines = spark.sparkContext.textFile("SampleInputData/hsbc_file.txt")
    val clickStreamDS = lines.map(mapper).toDF("EVENTS").cache()
    //val clickStreamDS = lines.toDF("EVENTS").cache()
    println("Here is our inferred schema:")
    clickStreamDS.printSchema()
    println("\n\n\n")

    import org.apache.spark.sql.functions._
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val clickStreamWithTimeStmpDS = clickStreamDS.toDF()




    val structuredDF = clickStreamWithTimeStmpDS.withColumn("EVENTS", explode($"EVENTS"))
    println("New Schema After Exploding the Array! ")
    structuredDF.printSchema()
    structuredDF.show(false)
    structuredDF.createOrReplaceTempView("events")



    println(" Describing the Events SQL Table  ")
    spark.sql("describe table events").show(false)



    //1.	What are the total number of events?
    println("1. What are the total number of events? ")
    spark.sql("SELECT count(*) as TOTAL_NUMBER_OF_EVENTS FROM events").show(false)

    //  2.	What are the distinct product types?
    println(" 2.\tWhat are the distinct product types? ")
    spark.sql("select distinct EVENTS.tradeProduct as DISTINCT_PRODUCTS from EVENTS").show(false)


    // 3. For each product type, what is the total amount?
    println(" 3. For each product type, what is the total amount?")
    spark.sql("select  EVENTS.tradeProduct, sum(EVENTS.tradeAmount) as TOTAL_TRADE_AMOUNT from EVENTS group by EVENTS.tradeProduct ").show(false)


    // 4.	Can we see the events ordered by the time they occurred?
    println(" 4. Can we see the events ordered by the time they occurred?")

    spark.sql(
      " select  EVENTS.tradeProduct,  sort_array(collect_list(EVENTS.tradeTimeStamp  )) as TIME_THEY_OCCURED from " +
        "EVENTS group by EVENTS.tradeProduct " ).show(false)









    println("5. For a given event, get the related events which make up the trade? ")
    //TODO



    // 6.	Data needs to be saved into Hive for other consumers?
    println("6. Data needs to be saved into Hive for other consumers? ")
    //structuredDF.write.format("orc").saveAsTable("HIVE_TABLE_NAME")
    // or export to a folder in ORC format
    // or spark.sql("CREATE TABLE new_table_name STORED AS ORC  AS SELECT * from events")



    //7.	How do we get the minimum amount for each product type?
    println(" 7.	How do we get the minimum amount for each product type? ")
    spark.sql(
      " select  EVENTS.tradeProduct, min(EVENTS.tradeAmount ) as MINIMUM_TRADE_AMOUNT  from EVENTS group by EVENTS.tradeProduct " )
      .show(false)


    // //8.	How do we get the max amount for each product type?
    println(" 8.\tHow do we get the max amount for each product type? ")
    spark.sql(
      " select  EVENTS.tradeProduct, max(EVENTS.tradeAmount ) as MAXIMUM_TRADE_AMOUNT from " +
        "EVENTS group by EVENTS.tradeProduct " ).show(false)


    // 9.	How do we get the total amount for each trade, and the max/min across trades?
    println(" 9.\tHow do we get the total amount for each trade, and the max/min across trades? ")
    spark.sql(
      " select  EVENTS.tradeProduct,  sum(EVENTS.tradeAmount) AS TOTAL_AMOUNT, min(EVENTS.tradeAmount ) as MINIMUM_AMOUNT, max(EVENTS.tradeAmount ) as MAXIMUM_AMOUNT  from " +
        "EVENTS group by EVENTS.tradeProduct " )
      .show(false)



    // Graceful Shutdown
    spark.close()
  }

  /**
    *
    * @param line
    * @return
    */
  def mapper(line: String): Array[ClickStream] = {
    val recordsPipeDelimited = line.split('|') // Pipe delimited
    var clickStreamArr = Array[ClickStream]()

    recordsPipeDelimited.foreach(recordCommaDelimited=>{
      val record = recordCommaDelimited.split(",")
      import org.apache.spark.sql.functions.unix_timestamp

      clickStreamArr +:=  ClickStream( record(0).trim, convertToDate(record(1)), convertToFloat(record(2)))
    })
    clickStreamArr
  }

  def convertToFloat(strInput: String): Float ={
    //Do Validation
    try{  strInput.toFloat
    } catch {
      case e: Exception => 0
    }
  }

  def convertToDate(strInput: String): Timestamp ={
    //Do Validation

    var timestamp:Timestamp=null

    try {
      import java.sql.Timestamp
      import java.text.SimpleDateFormat
      val dateFormat = new SimpleDateFormat("ddMMYYYY':'hh:mm")
      val parsedDate = dateFormat.parse(strInput)
      timestamp = new Timestamp(parsedDate.getTime)

    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        println("Date Format Not Supported")
        timestamp= null
    }
    timestamp
  }

  case class ClickStream(tradeProduct: String, tradeTimeStamp: Timestamp, tradeAmount: Float)
}
