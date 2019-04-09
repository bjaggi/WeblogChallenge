package com.eva.app

object PaytmUtils {

  /**
    *
    * @param line
    * @return
    */
  def mapper(line:String): ClickStream = {
    val parts = line.split(" ")

    val clickStream:ClickStream = new ClickStream(parts(0), parts(2), parts(5), parts(12))
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
case class ClickStream(timeStamp:String, clientIpPort : String, backendProcessingTime : String , url : String )

