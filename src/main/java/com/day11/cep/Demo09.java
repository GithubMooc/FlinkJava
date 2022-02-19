package com.day11.cep;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 02:18
 * @Name FlinkJava
 * <p>
 * 单个模式：条件
 * 对每个模式你可以指定一个条件来决定一个进来的事件是否被接受进入这个模式，例如前面用到的where就是一种条件
 * <p>
 * 单个模式：条件： 停止条件
 * 如果使用循环模式(oneOrMore, timesOrMore),在读无界流的时候，可以指定一个停止条件, 否则有可能会内存吃不消.
 * 意思是满足了给定的条件的事件出现后，就不会再有事件被接受进入模式了。
 */
public class Demo09 {
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
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .timesOrMore(2)
                .until(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc() >= 40;
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

