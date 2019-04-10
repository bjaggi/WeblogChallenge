package com.eva.app.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PaytmUtils {

  /**
    *
    * @param line
    * @return
    */
  def mapper(line: String): ClickStream = {
    val parts = line.split(" ") // space delimited

    val clickStream: ClickStream = new ClickStream(parts(0), parts(2), parts(5), parts(12))
    return clickStream
  }

}

class PaytmUtils {}


/**
  *
  * @param timeStamp
  * @param clientIpPort
  * @param backendProcessingTime
  * @param url
  */
case class ClickStream(timeStamp: String, clientIpPort: String, backendProcessingTime: String, url: String)

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

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