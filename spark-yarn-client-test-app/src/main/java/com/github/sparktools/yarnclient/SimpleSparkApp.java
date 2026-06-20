package com.github.sparktools.yarnclient;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Minimal Spark word-count application used in integration tests.
 * Reads text from {@code args[0]}, counts words, writes result to {@code args[1]}.
 */
public class SimpleSparkApp {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SimpleSparkApp <inputPath> <outputPath>");
            System.exit(1);
        }
        String inputPath  = args[0];
        String outputPath = args[1];

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        try {
            sc.textFile(inputPath)
              .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
              .filter(w -> !w.isEmpty())
              .mapToPair(w -> new Tuple2<>(w, 1))
              .reduceByKey(Integer::sum)
              .saveAsTextFile(outputPath);
        } finally {
            sc.stop();
        }
    }
}
