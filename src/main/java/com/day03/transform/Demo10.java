package com.day03.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:05
 * @Name FlinkJava
 *
 * transform:KeySelector
 * Lambda
 */
public class Demo10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(10, 3, 5, 9, 20, 8)
                .keyBy(value -> value % 2 == 0 ? "偶数" : "奇数")
                .print();
        env.execute();
    }
}
