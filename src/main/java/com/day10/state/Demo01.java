package com.day10.state;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/13
 * @Time 01:04
 * @Name FlinkJava
 *
 * State:Managed State:Keyed State: ValueState<T>
 * 保存单个值. 每个有key有一个状态值.  设置使用 update(T), 获取使用 T value()
 *
 * 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(3);
        env
                .socketTextStream("hadoop102", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        Integer lastVc = state.value() == null ? 0 : state.value();
                        if (Math.abs(value.getVc() - lastVc) >= 10) {
                            out.collect(value.getId() + " 红色警报!!!");
                        }
                        state.update(value.getVc());
                    }
                })
                .print();
        env.execute();
    }
}