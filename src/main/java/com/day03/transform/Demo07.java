package com.day03.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:05
 * @Name FlinkJava
 * <p>
 * transform:FilterFunction
 * 匿名内部类
 */
public class Demo07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(10, 3, 5, 9, 20, 8)
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value % 2 == 0;
                    }
                })
                .print();
        env.execute();
    }
}
