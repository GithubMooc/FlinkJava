package com.day03.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:05
 * @Name FlinkJava
 *
 * transform:FlatMapFunction
 * Lambda
 *
 * 在使用Lambda表达式表达式的时候, 由于泛型擦除的存在, 在运行的时候无法获取泛型的具体类型, 全部当做Object来处理, 极其低效, 所以Flink要求当参数中有泛型的时候, 必须明确指定泛型的类型.
 */
public class Demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1, 2, 3, 4, 5)
                .flatMap((Integer value, Collector<Integer> out) -> {
                    out.collect(value * value);
                    out.collect(value * value * value);
                }).returns(Types.INT)
                .print();
        env.execute();
    }
}
