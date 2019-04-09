package com.eva.app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.eva.app.PaytmUtils.mapper

object PaytmStreamingAnalytics {

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors ( For Development Only )
    //Logger.getLogger("org").setLevel(Level.ERROR)

    // Create the context
    val ssc = new StreamingContext("local[2]", "PageViewStream", Seconds(10))

    val pageViews = ssc.textFileStream("data/")
      //.flatMap(_.split(" "))
    .map(line => {
      println(line)
      val parts = line.split(" ")
      val cs = ClickStream(parts(0), parts(2), parts(5), parts(12))
      println(cs)
      cs

    })
    pageViews.foreachRDD(println(_))
    val pageCounts = pageViews.map(view => view.url).countByValue()
println(pageCounts)


    // Return a sliding window of page views per URL in the last ten seconds
    val slidingPageCounts = pageViews.map(view => view.url)
      .countByValueAndWindow(Seconds(10), Seconds(10))


    ssc.start()
    ssc.awaitTermination()
  }

}
