package com.day05.window;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * @Author Master
 * @Date 2022/2/10
 * @Time 20:47
 * @Name FlinkJava
 * <p>
 * 基于时间的窗口： 滚动窗口(Tumbling Windows)
 * <p>
 * 1.	时间间隔可以通过: Time.milliseconds(x), Time.seconds(x), Time.minutes(x),等等来指定.
 * 2.	我们传递给window函数的对象叫窗口分配器.
 */
public class Demo99 {

    public static void main(String[] args) throws Exception {
        AtomicInteger aaa= new AtomicInteger();
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
//        DataStreamSource<String> streamSource = env.readTextFile("input/words.txt");

        //3.将数据转为Tuple
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneStream = streamSource.flatMap((String value,Collector<Tuple2<String,Long>> out) -> {out.collect(Tuple2.of(value, 1L));}).returns(Types.TUPLE(Types.STRING,Types.LONG));

//        wordToOneStream.print("word");
        //4.将相同的单词聚合到同一个分区
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOneStream.keyBy(0);

        //TODO 5.开启一个基于时间的滚动窗口
        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> window = keyedStream.countWindow(3,2);
//        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> window = (WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow>) window1;


        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, GlobalWindow>() {
            @Override
            public void process(Tuple tuple, ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, GlobalWindow>.Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) {
//                System.out.println("tuple:"+tuple);
                String msg = "窗口一共有 " + iterable.spliterator().estimateSize() + "条数据 ";
                collector.collect(msg);
            }
        });

        process.print("process");
        window.sum(1).print("window sum");

        env.execute();
    }
}
