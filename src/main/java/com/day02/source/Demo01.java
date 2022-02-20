package com.day02.source;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 22:45
 * @Name FlinkJava
 *
 * 创建不同类型的执行环境
 *
 * 从Java的集合中读取数据
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));


        // 返回本地执行环境， 需要在调用时指定默认的并行度。
        // LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1);
        // LocalEnvironment localEnvironment1 = ExecutionEnvironment.createLocalEnvironment(1);

        // 返回集群执行环境， 将 Jar 提交到远程服务器。 需要在调用时指定 JobManager 的 IP 和端口号， 并指定要在集群中运行的 Jar 包。
        // ExecutionEnvironment localhost = ExecutionEnvironment.createRemoteEnvironment("localhost", 9999, "YOURPATH//wordcount.jar");
        // StreamExecutionEnvironment localhost1 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 9999, "YOURPATH//wordcount.jar");
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromCollection(waterSensors)
                .print();
        env.execute();
    }
}
