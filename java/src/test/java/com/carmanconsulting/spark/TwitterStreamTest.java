package com.carmanconsulting.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;
import java.util.List;

public class TwitterStreamTest {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: TwitterStreamTest <consumer key> <consumer secret> <access token> <access token secret> [<filters>]");
            System.exit(1);
        }

        final Configuration configuration = new ConfigurationBuilder().setOAuthConsumerKey(args[0]).setOAuthConsumerSecret(args[1]).setOAuthAccessToken(args[2]).setOAuthAccessTokenSecret(args[3]).build();
        final Authorization authorization = new OAuthAuthorization(configuration);
        final SparkConf conf = new SparkConf().setAppName("TwitterStreamTest").setMaster("local[*]");
        final JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(2));
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc, authorization, new String[0]);
        JavaDStream<String> hashTags = stream.flatMap(status -> Arrays.asList(status.getText().split(" "))).filter(s -> s.startsWith("#"));
        JavaPairDStream<Integer, String> topCounts10 = hashTags
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKeyAndWindow((left, right) -> left + right, Durations.seconds(10))
                .mapToPair(Tuple2::swap)
                .transformToPair(rdd -> rdd.sortByKey(false));

        topCounts10.foreachRDD(rdd -> {
            List<Tuple2<Integer, String>> topList = rdd.take(10);
            System.out.printf("%nPopular topics in last 10 seconds (%s total):%n", rdd.count());
            topList.forEach(pair -> System.out.printf("%s (%s tweets)%n", pair._2(), pair._1()));
            return null;
        });

        ssc.start();
        ssc.awaitTermination();

    }
}
