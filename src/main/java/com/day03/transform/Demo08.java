package com.day03.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:05
 * @Name FlinkJava
 *
 * transform:FilterFunction
 * Lambda
 */
public class Demo08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(10, 3, 5, 9, 20, 8)
                .filter(value -> value % 2 == 0)
                .print();
        env.execute();
    }
}
