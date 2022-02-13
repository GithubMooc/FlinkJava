package com.day08.processfunctionapi;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author Master
 * @Date 2022/2/12
 * @Time 22:07
 * @Name FlinkJava
 * <p>
 * JoinFunction
 */
public class Demo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> s1 = env
                // 在socket终端只输入毫秒级别的时间戳
                .socketTextStream("hadoop102", 8888)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                });
        SingleOutputStreamOperator<WaterSensor> s2 = env
                // 在socket终端只输入毫秒级别的时间戳
                .socketTextStream("hadoop102", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                });

        s1.join(s2)
                .where(WaterSensor::getId)
                .equalTo(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 必须使用窗口
                .apply((first, second) -> "first: " + first + ", second: " + second)
                .print();
        env.execute();
    }
}
