package com.day07.sideoutput;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.*;

/**
 * @Author Master
 * @Date 2022/2/11
 * @Time 01:13
 * @Name FlinkJava
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> result =
                env
                        .socketTextStream("localhost", 9999)
                        .map(value -> {
                            String[] datas = value.split(",");
                            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                        })
                        .keyBy(WaterSensor::getTs)
                        .process(new KeyedProcessFunction<Long, WaterSensor, WaterSensor>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) {
                                out.collect(value);
                                if (value.getVc() > 5) { //水位大于5的写入到侧输出流
                                    ctx.output(new OutputTag<WaterSensor>("警告") {
                                    }, value);
                                }
                            }
                        });
        result.print("主流");
        result.getSideOutput(new OutputTag<WaterSensor>("警告") {
        }).print("警告");
        env.execute();
    }
}
