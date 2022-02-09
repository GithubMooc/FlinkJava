package com.day03.transform;

import com.pojo.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:05
 * @Name FlinkJava
 *
 * transform:ProcessFunction
 * 在keyBy之前的流上使用
 */
public class Demo19 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        env
                .fromCollection(waterSensors)
                .process(new ProcessFunction<WaterSensor, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(value.getId(), value.getVc()));
                    }
                })
                .print();
        env.execute();
    }
}
