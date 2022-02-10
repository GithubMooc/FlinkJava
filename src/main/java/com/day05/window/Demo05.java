package com.day05.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/10
 * @Time 21:11
 * @Name FlinkJava
 * 基于时间的窗口： 全局窗口(Global Windows)
 */
public class Demo05 {
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

        //TODO 5.开启一个基于时间的滑动窗口
        WindowedStream<Tuple2<String, Long>, String, GlobalWindow> window = keyedStream.window(GlobalWindows.create());


        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>.Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                String msg = "窗口一共有 " + iterable.spliterator().estimateSize() + "条数据 ";
                collector.collect(msg);
            }
        });
        process.print();
        window.sum(1).print();

        env.execute();


    }
}