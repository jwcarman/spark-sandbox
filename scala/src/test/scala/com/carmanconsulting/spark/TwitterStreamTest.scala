package com.carmanconsulting.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.{OAuthAuthorization, Authorization}
import twitter4j.conf.{Configuration, ConfigurationBuilder}

object TwitterStreamTest extends App {

  if (args.length < 4) {
    System.err.println("Usage: TwitterStreamTest <consumer key> <consumer secret> <access token> <access token secret> [<filters>]")
    System.exit(1)
  }

  val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
  val filters = args.takeRight(args.length - 4)

  val configuration: Configuration = new ConfigurationBuilder().setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret).build()
  val authorization: Authorization = new OAuthAuthorization(configuration)
  val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  val stream = TwitterUtils.createStream(ssc, Some(authorization), filters)

  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

  val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    .map { case (topic, count) => (count, topic) }
    .transform(_.sortByKey(false))

  topCounts10.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
    topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
  })

  ssc.start()
  ssc.awaitTermination()
}
