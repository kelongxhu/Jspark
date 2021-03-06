package com.codethink.spark.sql.dataframe;

import com.alibaba.fastjson.JSON;
import com.codethink.common.Config;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.saveToEsWithMeta;

import java.util.*;

/**
 * @author codethink
 * @date 5/24/16 3:40 PM
 */
public class DfTest {


    public static void main(String[] args) {
        //初始化环境
        final SparkConf conf = new SparkConf().setAppName("test2");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final HiveContext sqlContext = new HiveContext(sc);
        JavaRDD<String> distFile = sc.textFile("/data/log/device_1.log");
        //文本行转换为对象
        JavaRDD<Device> objectRdd = distFile.flatMap(new FlatMapFunction<String, Device>() {
            @Override
            public Iterable<Device> call(String s) throws Exception {
                Device json = JSON.parseObject(s, Device.class);
                System.out.print("json:" + json);
                return Collections.singletonList(json);
            }
        });

        //对象转化为行
        JavaRDD<Row> rowRDD = objectRdd.map((Device device) -> {
            return RowFactory.create(device.getImei(), device.getApp());
        });

        //组装DataFrame
        StructType schema = DataTypes
            .createStructType(new StructField[] {
                DataTypes.createStructField("imei", DataTypes.StringType, true),
                DataTypes.createStructField("app", DataTypes.StringType, true)});


        final DataFrame deviceDF = sqlContext.createDataFrame(rowRDD, schema);
        deviceDF.registerTempTable("device");
        DataFrame dataFrame =
            sqlContext.sql("SELECT imei,app FROM device where imei=100 and app=1");

        List<Row> collect = dataFrame.javaRDD().collect();
        for (Row lists : collect) {
            String imei = lists.getAs("imei");
            String app = lists.getAs("app");
            System.out.println("imei:" + imei);
            System.out.println("app:" + app);
            System.out.println("=============list:" + lists);
        }

        //组装入库ES RDD....
        final JavaPairRDD<String, Map<Object, Object>> esRdd =
            dataFrame.toJavaRDD().mapToPair(new PairFunction<Row, String, Map<Object, Object>>() {
                @Override
                public Tuple2<String, Map<Object, Object>> call(final Row row)
                    throws Exception {
                    String imei = row.getAs("imei");
                    String app = row.getAs("app");
                    final Map<Object, Object> map = new HashMap<>();
                    map.put("imei", imei);
                    map.put("app", app);
                    return new Tuple2<>(imei + "_" + app, map);
                }
            }).coalesce(7);

        //入库ES
        saveToEsWithMeta(esRdd, "cat_test/app",
            Config.getEsConfigMap());
    }



    private static StructType getStructType(Schema[] schemas) {
        List<StructField> fields = new ArrayList<StructField>();
        for (Schema schema : schemas) {
            for (Schema.Field field : schema.getFields()) {
                field.schema().getType();
                fields.add(DataTypes.createStructField(field.name().toLowerCase(), getDataTypeForAvro(field.schema()),
                    true));
            }
        }

        return DataTypes.createStructType(fields);
    }


    private static DataType getDataTypeForAvro(Schema schema) {
        DataType returnDataType = DataTypes.StringType;

        switch (schema.getType()) {
            case INT:
                returnDataType = DataTypes.IntegerType;
                break;
            case STRING:
                returnDataType = DataTypes.StringType;
                break;
            case BOOLEAN:
                returnDataType = DataTypes.BooleanType;
                break;
            case BYTES:
                returnDataType = DataTypes.ByteType;
                break;
            case DOUBLE:
                returnDataType = DataTypes.DoubleType;
                break;
            case FLOAT:
                returnDataType = DataTypes.FloatType;
                break;
            case LONG:
                returnDataType = DataTypes.LongType;
                break;
            case FIXED:
                returnDataType = DataTypes.BinaryType;
                break;
            case ENUM:
                returnDataType = DataTypes.StringType;
                break;
        }

        return returnDataType;
    }
}
