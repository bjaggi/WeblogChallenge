package test.scala.com.eva.app
/*

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.junit.Before
import org.junit.After
import com.eva.app.WordCountProcessor
import org.apache.spark.rdd.RDD
import org.junit.Assert
import scala.collection.mutable.ArrayBuffer

class WordCountTest extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = _
  // val inputList = List(Magic("panda", 9001.0, byteArray), Magic("coffee", 9002.0, byteArray))
  val fileLines = Array("Line One", "Line Two", "Line Three", "Line Four")

  before {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    sc = new SparkContext(conf)
    println(" =====>initializing spark context ")

  }

  // example soure : http://beekeeperdata.com/posts/hadoop/2015/12/14/spark-scala-tutorial.html
  //http://www.slideshare.net/knoldus/unit-testing-of-spark-applications
  //http://techblog.newsweaver.com/spark-tdd/    
  test("splitLines should split multi-word lines into words") {
    val inputRDD: RDD[String] = sc.parallelize[String](fileLines)
    val wordCount = new com.eva.app.WordCountProcessor
    val wordsRDD = wordCount.getWordsByLine(inputRDD)
    //wordsRDD.count() should be ("8")
    Assert.assertEquals(wordsRDD.count(), 8)

  }

  test("testing word count") {
    val inputRDD: RDD[String] = sc.parallelize[String](fileLines)
    val wordCount = new com.eva.app.WordCountProcessor
    val wordsRDD = wordCount.getWordsByLine(inputRDD)
    //wordsRDD.count() should be ("8")
    Assert.assertEquals(wordsRDD.count(), 8)
    val resultRDD = wordsRDD.map(word => (word, 1)).reduceByKey(_ + _)
    resultRDD.foreach(println)

    println(".....====>   " + resultRDD.lookup("Line"))
    Assert.assertEquals(resultRDD.lookup("Line"), ArrayBuffer(4))
    Assert.assertEquals(resultRDD.lookup("One"), ArrayBuffer(1))

  }

  after {
    sc.stop()
  }

}*/
