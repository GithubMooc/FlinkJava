package com.day08.processfunctionapi;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/12
 * @Time 22:05
 * @Name FlinkJava
 *
 * CoProcessFunction
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");

        ConnectedStreams<Integer, String> cs = intStream.connect(stringStream);
        cs
                .process(new CoProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement1(Integer value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toString());
                    }

                    @Override
                    public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value);
                    }
                })
                .print();

        env.execute();
    }
}
