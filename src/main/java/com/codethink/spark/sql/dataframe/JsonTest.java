package com.codethink.spark.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author codethink
 * @date 5/31/16 6:00 PM
 */
public class JsonTest {
    public static void main(String[] args) {
        //初始化环境
        final SparkConf conf = new SparkConf().setAppName("test2");
        conf.setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(ctx);
        // 换一个json数据结构的例子
        List<String>
            jsonData = Arrays.asList("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}",
            "{\"name\":\"LongKe\",\"address\":{\"city\":\"Hy\",\"state\":\"China\"}}");
        JavaRDD<String> anotherPeopleRDD = ctx.parallelize(jsonData);
        DataFrame peopleFromJsonRDD = sqlContext.read().json(anotherPeopleRDD.rdd());

        // 打印出新的数据结构
        peopleFromJsonRDD.printSchema();
        // 打印如下：
        // root
        // |-- address: struct (nullable = true)
        // | |-- city: string (nullable = true)
        // | |-- state: string (nullable = true)
        // |-- name: string (nullable = true)

        // 将DataFrame注册为一个Table
        peopleFromJsonRDD.registerTempTable("people2");

        // 执行SQL查询
        DataFrame peopleWithCity = sqlContext.sql("SELECT name, address.city FROM people2");
        List<String> nameAndCity = peopleWithCity.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0) + ", City: " + row.getString(1);
            }
        }).collect();
        for (String name : nameAndCity) {
            System.out.println(name);
        }
    }
}
