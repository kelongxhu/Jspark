package com.codethink.spark.sql.jdbc;


import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
/**
 * @author codethink
 * @date 5/26/16 11:17 AM
 */
public class JdbcMain implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMain.class);

    private static final JavaSparkContext sc =
        new JavaSparkContext(new SparkConf().setAppName("SparkJdbc").setMaster("local[*]"));

    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/aota";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "123456";

    public static void main(String[] args) {
        DbConnection dbConnection = new DbConnection(MYSQL_DRIVER, MYSQL_CONNECTION_URL, MYSQL_USERNAME, MYSQL_PWD);

        // Load data from MySQL
        JdbcRDD<Object[]> jdbcRDD =
            new JdbcRDD<>(sc.sc(), dbConnection, "select id,app_name,apk_pack_name from aota_app where id >= ?", 1,
                10, 10, new MapResult(), ClassManifestFactory$.MODULE$.fromClass(Object[].class));

        // Convert to JavaRDD
        JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));

        // Join first name and last name
        List<String> employeeFullNameList = javaRDD.map(new Function<Object[], String>() {
            @Override
            public String call(final Object[] record) throws Exception {
                return record[1] + " " + record[2];
            }
        }).collect();

        for (String fullName : employeeFullNameList) {
            LOGGER.info("=============================:{}",fullName);
        }
    }

    static class DbConnection extends AbstractFunction0<Connection> implements Serializable {

        private String driverClassName;
        private String connectionUrl;
        private String userName;
        private String password;

        public DbConnection(String driverClassName, String connectionUrl, String userName, String password) {
            this.driverClassName = driverClassName;
            this.connectionUrl = connectionUrl;
            this.userName = userName;
            this.password = password;
        }

        @Override
        public Connection apply() {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                LOGGER.error("Failed to load driver class", e);
            }

            Properties properties = new Properties();
            properties.setProperty("user", userName);
            properties.setProperty("password", password);

            Connection connection = null;
            try {
                connection = DriverManager.getConnection(connectionUrl, properties);
            } catch (SQLException e) {
                LOGGER.error("Connection failed", e);
            }

            return connection;
        }
    }

    static class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {

        public Object[] apply(ResultSet row) {
            return JdbcRDD.resultSetToObjectArray(row);
        }
    }
}
