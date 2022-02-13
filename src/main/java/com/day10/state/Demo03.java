package com.day10.state;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/13
 * @Time 01:14
 * @Name FlinkJava
 * <p>
 * State:Managed State:Keyed State: ReducingState<T>:
 * 存储单个值, 表示把所有元素的聚合结果添加到状态中.  与ListState类似, 但是当使用add(T)的时候ReducingState会使用指定的ReduceFunction进行聚合.
 * <p>
 * 计算每个传感器的水位和
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(3);
        env
                .socketTextStream("localhost", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

                    //定义状态
                    private ReducingState<WaterSensor> reducingState;

                    //在声明周期open方法中初始化状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("reducing-state", (ReduceFunction<WaterSensor>) (value1, value2) -> new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc()), WaterSensor.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        //将当前数据聚合进状态
                        reducingState.add(value);

                        //取出状态中的数据
                        WaterSensor waterSensor = reducingState.get();

                        //输出数据
                        out.collect(waterSensor);
                    }
                })
                .print();
        env.execute();
    }
}