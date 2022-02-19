package com.day02.source;

import com.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 22:56
 * @Name FlinkJava
 *
 * 自定义Source
 *
 * 自定义 SourceFunction：
 *       1. 实现 SourceFunction相关接口
 *       2. 重写两个方法：
 *               run(): 主要逻辑
 *               cancel(): 停止逻辑
 *
 *    如果希望 Source可以指定并行度，那么就 实现 ParallelSourceFunction 这个接口
 */
public class Demo05 {
    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new MySource("hadoop102", 9999))
                .print();

        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private String host;
        private int port;
        private volatile boolean isRunning = true;
        private Socket socket;

        public MySource(String host, int port) {
            this.host = host;
            this.port = port;
        }


        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            // 实现一个从socket读取数据的source
            socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line = null;
            while (isRunning && (line = reader.readLine()) != null) {
                String[] split = line.split(",");
                ctx.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
            }
        }

        /**
         * 大多数的source在run方法内部都会有一个while循环,
         * 当调用这个方法的时候, 应该可以让run方法中的while循环结束
         */

        @Override
        public void cancel() {
            isRunning = false;
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
