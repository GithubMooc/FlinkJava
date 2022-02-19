package com.day23.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 23:22
 * @Name FlinkJava
 * <p>
 * SQL API中使用窗口：Over Windows
 */
public class Demo11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 作为事件时间的字段必须是 timestamp 类型, 所以根据 long 类型的 ts 计算出来一个 t
        tEnv.executeSql(
                "create table sensor(" +
                        "id string," +
                        "ts bigint," +
                        "vc int, " +
                        "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        "watermark for t as t - interval '5' second)" +
                        "with(" +
                        "'connector' = 'filesystem'," +
                        "'path' = 'input/sensor.txt'," +
                        "'format' = 'csv'" +
                        ")");

        tEnv
                .sqlQuery(
                        "select " +
                                "id," +
                                "vc," +
                                "count(vc) over w, " +
                                "sum(vc) over w " +
                                "from sensor " +
                                "window w as (partition by id order by t rows between 1 PRECEDING and current row)"
                )
                .execute()
                .print();
    }
}