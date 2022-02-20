package com.day01.introduction;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 22:37
 * @Name FlinkJava
 */

public class Demo04 {
    public static void main(String[] args) throws Exception {
        //使用本地模式并开启WebUI
        Configuration conf = new Configuration();
        //端口绑定给定一个范围，由小到大尝试使用端口，如果被占用则用下一个端口,访问地址：http://localhost/8081
        conf.setString(RestOptions.BIND_PORT,"8081-8089");
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 2. 读取文件
        DataStreamSource<String> lineDSS = env.socketTextStream("hadoop102", 9999);
        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
                .keyBy(t -> t.f0);
        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
                .sum(1);
        // 6. 打印
        result.print();
        // 7. 执行
        env.execute();
    }
}
