package com.carmanconsulting.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCountTest extends Assert {

    private JavaSparkContext context;

    @Before
    public void initSparkContext() {
        SparkConf conf = new SparkConf().setAppName(getClass().getSimpleName()).setMaster("local");
        context = new JavaSparkContext(conf);
    }

    @After
    public void shutdownSparkContext() {
        context.stop();
    }

    @Test
    public void testWithOneWord() {
        JavaRDD<String> file = context.textFile("../gettysburg.txt");
        JavaRDD<String> words = file.flatMap(line -> Arrays.asList(line.split(" ")));
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        Map<String, Integer> map = counts.collectAsMap();
        assertEquals(1, map.get("Four").longValue());
    }

}
