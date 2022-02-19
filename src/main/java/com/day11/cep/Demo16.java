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
 * <p>
 * 循环模式的贪婪性
 * 在组合模式情况下, 对次数的处理尽快能获取最多个的那个次数, 就是贪婪!当一个事件同时满足两个模式的时候起作用.
 *
 * 数据:
 * sensor_1,1,10
 * sensor_1,2,20
 * sensor_1,3,30
 * sensor_2,4,30
 * sensor_1,4,400
 * sensor_2,5,50
 * sensor_2,6,60
 *
 * 结果:
 * {start=[WaterSensor(id=sensor_1, ts=1, vc=10), WaterSensor(id=sensor_1, ts=2, vc=20), WaterSensor(id=sensor_1, ts=3, vc=30)], end=[WaterSensor(id=sensor_2, ts=4, vc=30)]}
 *
 * {start=[WaterSensor(id=sensor_1, ts=2, vc=20), WaterSensor(id=sensor_1, ts=3, vc=30)], end=[WaterSensor(id=sensor_2, ts=4, vc=30)]}
 *
 */
public class Demo16 {
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
                .<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                }).times(2, 3).greedy()
                .next("end")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc() == 30;
                    }
                });

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

