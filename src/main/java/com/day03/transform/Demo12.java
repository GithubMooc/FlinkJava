package com.day03.transform;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:05
 * @Name FlinkJava
 *
 * transform:connect
 * 1.	两个流中存储的数据类型可以不同
 * 2.	只是机械的合并在一起, 内部仍然是分离的2个流
 * 3.	只能2个流进行connect, 不能有第3个参与
 */
public class Demo12 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");
        // 把两个流连接在一起: 貌合神离
        ConnectedStreams<Integer, String> cs = intStream.connect(stringStream);
        cs.getFirstInput().print("first");
        cs.getSecondInput().print("second");
        env.execute();
        env.execute();
    }
}
