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
 * transform:简单滚动聚合算子：maxBy,minBy
 * 滚动聚合算子： 来一条，聚合一条
 *         1、聚合算子在 keyby之后调用，因为这些算子都是属于 KeyedStream里的
 *         2、聚合算子，作用范围，都是分组内。 也就是说，不同分组，要分开算。
 *         3、max、maxBy的区别：
 *             max：取指定字段的当前的最大值，如果有多个字段，其他非比较字段，以第一条为准
 *             maxBy：取指定字段的当前的最大值，如果有多个字段，其他字段以最大值那条数据为准；
 *             如果出现两条数据都是最大值，由第二个参数决定： true => 其他字段取 比较早的值； false => 其他字段，取最新的值
 */
public class Demo16 {
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
                .maxBy("vc", false)
                .print("maxBy...");
        env.execute();
    }
}
