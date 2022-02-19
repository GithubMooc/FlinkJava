package com.day11.cep;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 02:22
 * @Name FlinkJava
 *
 * 模式组
 * 在前面的代码中次数只能用在某个模式上, 比如: .begin(...).where(...).next(...).where(...).times(2)  这里的次数只会用在next这个模式上, 而不会用在begin模式上.
 * 如果需要用在多个模式上,可以使用模式组!
 *
 * 结果:
 * {begin=[WaterSensor(id=sensor_1, ts=2000, vc=20), WaterSensor(id=sensor_1, ts=4000, vc=40)], next=[WaterSensor(id=sensor_2, ts=3000, vc=30), WaterSensor(id=sensor_2, ts=5000, vc=50)]}
 */
public class Demo18 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0],
                                Long.parseLong(split[1]) * 1000,
                                Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTs()));
        // 1. 定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .begin(Pattern
                        .<WaterSensor>begin("start")
                        .where(new SimpleCondition<WaterSensor>() {
                            @Override
                            public boolean filter(WaterSensor value) throws Exception {
                                return "sensor_1".equals(value.getId());
                            }
                        })
                        .next("next")
                        .where(new SimpleCondition<WaterSensor>() {
                            @Override
                            public boolean filter(WaterSensor value) throws Exception {
                                return "sensor_2".equals(value.getId());
                            }
                        }))
                .times(2);

        // 2. 在流上应用模式
        PatternStream<WaterSensor> waterSensorPS = CEP.pattern(waterSensorStream, pattern);
        // 3. 获取匹配到的结果
        waterSensorPS
                .select(pattern1 -> pattern1.toString())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

