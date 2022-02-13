package com.day10.state;

import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/13
 * @Time 02:09
 * @Name FlinkJava
 *
 * State:Managed State:Operator State: BroadcastState<T>: 广播状态
 *
 * 是一种特殊的算子状态. 如果一个算子有多项任务，而它的每项任务状态又都相同，那么这种特殊情况最适合应用广播状态。
 *
 * 从版本1.5.0开始，Apache Flink具有一种新的状态，称为广播状态。
 * 广播状态被引入以支持这样的用例:来自一个流的一些数据需要广播到所有下游任务，在那里它被本地存储，并用于处理另一个流上的所有传入元素。作为广播状态自然适合出现的一个例子，我们可以想象一个低吞吐量流，其中包含一组规则，我们希望根据来自另一个流的所有元素对这些规则进行评估。考虑到上述类型的用例，广播状态与其他操作符状态的区别在于:
 * 1. 它是一个map格式。
 * 2. 它只对输入有广播流和无广播流的特定操作符可用。
 * 3. 这样的操作符可以具有不同名称的多个广播状态。
 */
public class Demo08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(3);
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop102", 8888);
        // 定义状态并广播
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);
        // 广播流
        BroadcastStream<String> broadcastStream = controlStream.broadcast(stateDescriptor);
        dataStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // 从广播状态中取值, 不同的值做不同的业务
                        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                        if ("1".equals(state.get("switch"))) {
                            out.collect("切换到1号配置....");
                        } else if ("0".equals(state.get("switch"))) {
                            out.collect("切换到0号配置....");
                        } else {
                            out.collect("切换到其他配置....");
                        }
                    }

                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
// 提取状态
                        BroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                        // 把值放入广播状态
                        state.put("switch", value);
                    }
                })
                .print();

        env.execute();
    }
}
