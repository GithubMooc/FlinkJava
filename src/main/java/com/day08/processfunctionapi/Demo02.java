package com.day08.processfunctionapi;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/12
 * @Time 22:02
 * @Name FlinkJava
 *
 * KeyedProcessFunction
 */
public class Demo02  {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] datas = line.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 泛型1:key的类型 泛型2:输入类型 泛型3:输出类型
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) {
                        System.out.println(ctx.getCurrentKey());
                        out.collect(value.toString());
                    }
                })
                .print();

        env.execute();
    }
}
