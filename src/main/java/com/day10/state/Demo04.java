package com.day10.state;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/13
 * @Time 01:41
 * @Name FlinkJava
 *
 * State:Managed State:Keyed State: AggregatingState<IN, OUT>:
 * 存储单个值. 与ReducingState类似, 都是进行聚合. 不同的是, AggregatingState的聚合的结果和元素类型可以不一样.
 *
 * 计算每个传感器的平均水位
 */
public class Demo04 {
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
                .process(new KeyedProcessFunction<String, WaterSensor, Double>() {


                    //定义状态
                    private AggregatingState<Integer, Double> avgState;

                    //在声明周期open方法中初始化状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        avgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>("avgState", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                            //创建累加器----->Tuple第一个元素放的是累加的值，第二个元素是数据个数
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0, 0);
                            }

                            //累加操作----->第一个元素将状态值加上当前值，第二个元素将数据个数+1
                            @Override
                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                            }

                            //求平均值------>元组第一个元素的值（累加值）除以元组第二个元素（数据个数）
                            @Override
                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                return accumulator.f0 * 1D / accumulator.f1;
                            }

                            //合并两个累加器
                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                            }
                        }, Types.TUPLE(Types.INT, Types.INT)));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Double> out) throws Exception {
                        //将当前数据累加进状态
                        avgState.add(value.getVc());
                        //取出状态中的数据
                        out.collect(avgState.get());
                    }
                })
                .print();
        env.execute();
    }
}