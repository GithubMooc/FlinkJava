package com.day06.watermark;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author Master
 * @Date 2022/2/11
 * @Time 01:04
 * @Name FlinkJava
 *
 * 窗口允许迟到的数据:.allowedLateness(Time.seconds(3))
 * 允许迟到只能运用在event time上
 */
public class Demo05 {public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

    SingleOutputStreamOperator<WaterSensor> stream = env
            // 在socket终端只输入毫秒级别的时间戳
            .socketTextStream("localhost", 9999)
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            });
    SingleOutputStreamOperator<WaterSensor> watermarks = stream
            .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                            .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                            .withTimestampAssigner((waterSensor, l) -> waterSensor.getTs() * 1000));
    watermarks
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .allowedLateness(Time.seconds(3))
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
        @Override
        public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) {
            String msg = "当前key: " + s + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 " + iterable.spliterator().estimateSize() + "条数据 ";
            collector.collect(msg);
        }
    }).print();
    env.execute();
}
}