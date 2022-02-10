package com.day05.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/10
 * @Time 21:32
 * @Name FlinkJava
 *
 * Window Function： ProcessWindowFunction(全窗口函数)
 */
public class Demo10 {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为Tuple
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneStream = streamSource.flatMap((String value, Collector<Tuple2<String,Long>>out) -> {
            String[] split = value.split(" ");
            for (String s : split) {
                out.collect(Tuple2.of(s, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));
        //4.将相同的单词聚合到同一个分区
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneStream.keyBy(a -> a.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> process = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                    // 参数1: key 参数2: 上下文对象 参数3: 这个窗口内所有的元素 参数4: 收集器, 用于向下游传递数据
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) {
                        System.out.println(context.window().getStart());
                        long sum = 0L;
                        for (Tuple2<String, Long> t : elements) {
                            sum += t.f1;
                        }
                        out.collect(Tuple2.of(key, sum));
                    }
                });

        process.print();

        env.execute();
    }
}