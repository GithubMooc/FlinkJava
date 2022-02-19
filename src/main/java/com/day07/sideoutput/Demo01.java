package com.day07.sideoutput;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.*;

import java.time.Duration;

/**
 * @Author Master
 * @Date 2022/2/11
 * @Time 01:10
 * @Name FlinkJava
 * <p>
 * 侧输出流(sideOutput):处理窗口关闭之后的迟到数据
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
//        System.out.println(env.getConfig());

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("localhost", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                });

        // 创建水印生产策略
        // 指定时间戳
        WatermarkStrategy<WaterSensor> wms = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)) // 最大容忍的延迟时间
                .withTimestampAssigner((element, l) -> element.getTs() * 1000);


        SingleOutputStreamOperator<String> result = stream
                .assignTimestampsAndWatermarks(wms)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                // 设置侧输出流
                .sideOutputLateData(new OutputTag<WaterSensor>("side_1") {
                })
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String msg = "当前key: " + key
                                + " 窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                + elements.spliterator().estimateSize() + "条数据" +
                                "watermark: " + context.currentWatermark();
                        out.collect(context.window().toString());
                        out.collect(msg);
                    }
                });
        result.print();
        result.getSideOutput(new OutputTag<WaterSensor>("side_1") {}).print();
        env.execute();
    }
}