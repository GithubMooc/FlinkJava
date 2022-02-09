package com.day03.transform;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:05
 * @Name FlinkJava
 *
 * transform:reduce
 * 1、 一个分组的第一条数据来的时候，不会进入reduce方法。
 * 2、 输入和输出的 数据类型，一定要一样。
 */
public class Demo18 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        KeyedStream<WaterSensor, String> kbStream = env
                .fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);
        kbStream
                .reduce((value1, value2) -> {
                    System.out.println("reducer function ...");
                    return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
                })
                .print("reduce...");
        env.execute();
    }
}
