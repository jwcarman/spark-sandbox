package com.carmanconsulting.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, After, Before, Test}

class WordCountTest {

  var context: SparkContext = _

  @Before
  def initSparkContext(): Unit = {
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local")
    context = new SparkContext(conf)
  }

  @After
  def shutdownSparkContext(): Unit = context.stop()

  @Test
  def testGettysburg() = {
    val file = context.textFile("../gettysburg.txt")
    val counts = file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    Assert.assertEquals(1, counts.collectAsMap().get("Four").get)
  }
}
