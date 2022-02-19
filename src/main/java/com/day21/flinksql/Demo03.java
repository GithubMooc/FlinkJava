package com.day21.flinksql;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 22:43
 * @Name FlinkJava
 * 查询已注册的表：从流得到一个表：注册为一个临时视图
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 使用sql查询一个已注册的表
        // 注册为一个临时视图
        tableEnv.createTemporaryView("inputTable", waterSensorStream);
        // 这种方式，如果需要 Table对象，可以从表名获取
        // Table sensorTable = tableEnv.from("inputTable");

        Table resultTable = tableEnv.sqlQuery("select * from inputTable where id='sensor_1'");
        // 显示resultTable的数据
        tableEnv.toAppendStream(resultTable, Row.class).print();
        env.execute();
    }
}
