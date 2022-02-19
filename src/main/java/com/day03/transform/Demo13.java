package com.day03.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:05
 * @Name FlinkJava
 *
 * transform:union
 *
 * connect与 union 区别：
 * 1.	union之前两个流的类型必须是一样，connect可以不一样
 * 2.	connect只能操作两个流，union可以操作多个。
 *
 * 总结：union 多个，一样
 *      connect 2个，可不一样
 */
public class Demo13 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> stream2 = env.fromElements(10, 20, 30, 40, 50);
        DataStreamSource<Integer> stream3 = env.fromElements(100, 200, 300, 400, 500);

        // 把多个流union在一起成为一个流, 这些流中存储的数据类型必须一样: 水乳交融
        stream1
                .union(stream2)
                .union(stream3)
                .print();
        env.execute();
    }
}
