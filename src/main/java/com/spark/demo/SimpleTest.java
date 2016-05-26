package com.spark.demo;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author codethink
 * @date 5/19/16 3:39 PM
 */
public class SimpleTest {
    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("test");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //求和
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        int total = distData.reduce((a, b) -> a + b);
        System.out.println("============" + total);
        //
        JavaRDD<String> distFile = sc.textFile("/data/test.log");
        JavaRDD<Integer> lineLengths=distFile.map(line->line.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println("============" + totalLength);
    }
}
