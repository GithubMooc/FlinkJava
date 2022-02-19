package com.day23.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 23:19
 * @Name FlinkJava
 * <p>
 * SQL API中使用窗口：Group Windows
 * <p>
 * SQL查询的分组窗口是通过GROUP BY子句定义的。类似于使用常规GROUP BY语句的查询，窗口分组语句的GROUP BY子句中带有一个窗口函数为每个分组计算出一个结果。
 */
public class Demo09 {
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
                        "SELECT id, " +
                                "  hop_start(t, INTERVAL '1' minute, INTERVAL '1' hour) as wStart,  " +
                                "  hop_end(t, INTERVAL '1' minute, INTERVAL '1' hour) as wEnd,  " +
                                "  SUM(vc) sum_vc " +
                                "FROM sensor " +
                                "GROUP BY hop(t, INTERVAL '1' minute, INTERVAL '1' hour), id"
                )
                .execute()
                .print();
    }
}