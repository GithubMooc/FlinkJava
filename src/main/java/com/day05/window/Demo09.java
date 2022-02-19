package com.day05.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/10
 * @Time 21:28
 * @Name FlinkJava
 *
 * Window Function： AggregateFunction(增量聚合函数----可以改变数据的类型)
 */
public class Demo09 {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为Tuple
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneStream = streamSource.flatMap((String value, Collector<Tuple2<String,Long>> out) -> {
            String[] split = value.split(" ");
            for (String s : split) {
                out.collect(Tuple2.of(s, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));
        //4.将相同的单词聚合到同一个分区
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneStream.keyBy(a -> a.f0);

        SingleOutputStreamOperator<Long> aggregate = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {

                    // 创建累加器: 初始化中间值
                    @Override
                    public Long createAccumulator() {
                        System.out.println("createAccumulator");
                        return 0L;
                    }

                    // 累加器操作
                    @Override
                    public Long add(Tuple2<String, Long> value, Long accumulator) {
                        System.out.println("add");
                        return accumulator + value.f1;
                    }

                    // 获取结果
                    @Override
                    public Long getResult(Long accumulator) {
                        System.out.println("getResult");
                        return accumulator;
                    }

                    // 累加器的合并: 只有会话窗口才会调用
                    @Override
                    public Long merge(Long a, Long b) {
                        System.out.println("merge");
                        return a + b;
                    }
                });
        aggregate.print();

        env.execute();
    }
}