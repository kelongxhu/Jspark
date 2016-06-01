package com.codethink.spark.sql.jdbc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

/**
 * @author codethink
 * @date 5/26/16 1:19 PM
 */
public class JdbcSave {
    private static final long serialVersionUID = -8513279306224995844L;
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "123456";
    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://127.0.0.1:3306/aota";

    private static final JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkSaveToDb").setMaster("local[*]"));

    private static final SQLContext sqlContext = new SQLContext(sc);

    public static void main(String[] args) {
        // Sample data-frame loaded from a JSON file
        DataFrame usersDf = sqlContext.read().json("/data/log/os.json");

        // Save data-frame to MySQL (or any other JDBC supported databases)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);

        // write dataframe to jdbc mysql
        usersDf.write().mode(SaveMode.Append).jdbc(MYSQL_CONNECTION_URL, "aota_strategy", connectionProperties);
    }
}
