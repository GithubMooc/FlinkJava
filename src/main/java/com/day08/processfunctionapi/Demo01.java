package com.day08.processfunctionapi;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/11
 * @Time 01:19
 * @Name FlinkJava
 * <p>
 * ProcessFunction
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] datas = line.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                })
                .process(new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toString());
                    }
                })
                .print();
        env.execute();
    }
}
