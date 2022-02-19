package com.day24.udf;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 23:35
 * @Name FlinkJava
 *
 * 用户定义函数（User-defined Functions，UDF）是一个重要的特性，因为它们显著地扩展了查询（Query）的表达能力。
 *
 * 聚合函数（Aggregate Functions）
 */
public class Demo07 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取文件得到DataStream
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDataStreamSource);

        //4不注册直接使用
        table.groupBy($("id"))
                .select($("id"),call(MyAvg.class,$("vc")))
                .execute()
                .print();
    }


    //定义一个类当做累加器，并声明总数和总个数这两个值
    public static class MyAvgAccumulator{
        public long sum = 0;
        public int count = 0;
    }

    //自定义UDAF函数,求每个WaterSensor中VC的平均值
    public static class MyAvg extends AggregateFunction<Double, MyAvgAccumulator> {

        //创建一个累加器
        @Override
        public MyAvgAccumulator createAccumulator() {
            return new MyAvgAccumulator();
        }

        //做累加操作
        public void accumulate(MyAvgAccumulator acc, Integer vc) {
            acc.sum += vc;
            acc.count += 1;

        }

        //将计算结果值返回
        @Override
        public Double getValue(MyAvgAccumulator accumulator) {
            return accumulator.sum*1D/accumulator.count;
        }
    }
}
