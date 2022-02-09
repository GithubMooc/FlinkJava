package com.day03.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:01
 * @Name FlinkJava
 *
 * transform:map
 * 静态内部类
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1, 2, 3, 4, 5)
                .map(new MyMapFunction())
                .print();
        env.execute();
    }
    public static class MyMapFunction implements MapFunction<Integer, Integer> {
        @Override
        public Integer map(Integer value) {
            return value * value;
        }
    }
}
