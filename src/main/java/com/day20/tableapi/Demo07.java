package com.day20.tableapi;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 02:46
 * @Name FlinkJava
 * <p>
 * 通过Connector声明写出数据：File Sink
 */
public class Demo07 {

        public static void main(String[] args) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStreamSource<WaterSensor> waterSensorStream =
                    env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                            new WaterSensor("sensor_1", 2000L, 20),
                            new WaterSensor("sensor_2", 3000L, 30),
                            new WaterSensor("sensor_1", 4000L, 40),
                            new WaterSensor("sensor_1", 5000L, 50),
                            new WaterSensor("sensor_2", 6000L, 60));
            // 1. 创建表的执行环境
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            Table sensorTable = tableEnv.fromDataStream(waterSensorStream);
            Table resultTable = sensorTable
                    .where($("id").isEqual("sensor_1") )
                    .select($("id"), $("ts"), $("vc"));

            // 创建输出表
            Schema schema = new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts", DataTypes.BIGINT())
                    .field("vc", DataTypes.INT());
            tableEnv
                    .connect(new FileSystem().path("output/sensor_id.txt"))
                    .withFormat(new Csv().fieldDelimiter('|'))
                    .withSchema(schema)
                    .createTemporaryTable("sensor");

            // 把数据写入到输出表中
            resultTable.executeInsert("sensor");
        }
    }
