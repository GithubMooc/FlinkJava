package com.day24.udf;

import com.pojo.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 23:38
 * @Name FlinkJava
 *
 * 用户定义函数（User-defined Functions，UDF）是一个重要的特性，因为它们显著地扩展了查询（Query）的表达能力。
 *
 * 表聚合函数（Table Aggregate Functions）
 */
public class Demo10 {
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

        //4先注册再使用
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        //TableAPI
        table.groupBy($("id"))
                .flatAggregate(call("Top2", $("vc")).as("top", "rank"))
                .select($("id"), $("top"), $("rank"))
                .execute()
                .print();

    }

    //定义一个类当做累加器，并声明第一和第二这两个值
    public static class vCTop2 {
        public Integer first = Integer.MIN_VALUE;
        public Integer second = Integer.MIN_VALUE;
    }


    //自定义UDATF函数（多进多出）,求每个WaterSensor中最高的两个水位值
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, vCTop2> {

        //创建累加器
        @Override
        public vCTop2 createAccumulator() {
            return new vCTop2();
        }

        //比较数据，如果当前数据大于累加器中存的数据则替换，并将原累加器中的数据往下（第二）赋值
        public void accumulate(vCTop2 acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        //计算（排名）
        public void emitValue(vCTop2 acc, Collector<Tuple2<Integer, Integer>> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }
}