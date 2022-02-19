package com.day23.window;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 23:08
 * @Name FlinkJava
 * <p>
 * Table API中使用窗口：Group Windows： 滚动窗口
 * <p>
 * 分组窗口（Group Windows）会根据时间或行计数间隔，将行聚合到有限的组（Group）中，并对每个组的数据执行一次聚合函数。
 * Table API中的Group Windows都是使用。Window（w:GroupWindow）子句定义的，并且必须由as子句指定一个别名。为了按窗口对表进行分组，窗口的别名必须在group by子句中，像常规的分组字段一样引用。
 *
 * 开窗四部曲：
 * 1.窗口类型：.window(Tumble.over(lit(10).second()).on($("ts")).as("w"))
 * 2.窗口相关参数：比如窗口大小
 * .window(Tumble.over(lit(10).second()).on($("ts")).as("w"))
 * 3.指定时间字段：.window(Tumble.over(lit(10).second()).on($("ts")).as("w"))
 * 4.窗口别名：.window(Tumble.over(lit(10).second()).on($("ts")).as("w"))
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv
                .fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));

        table
                // 定义滚动窗口并给窗口起一个别名
                .window(Slide.over(lit(10).second()).every(lit(5).second()).on($("ts")).as("w"))
                // 窗口必须出现的分组字段中
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").start(), $("w").end(), $("vc").sum())
                .execute()
                .print();
        env.execute();
    }
}
