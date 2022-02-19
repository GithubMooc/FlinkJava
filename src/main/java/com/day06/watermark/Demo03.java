package com.day06.watermark;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Author Master
 * @Date 2022/2/11
 * @Time 00:59
 * @Name FlinkJava
 * 自定义WatermarkStrategy:periodic(周期性)
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据并转为javaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(date -> {
                    String[] words = date.split(" ");
                    return new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
                });


        //3.自定义WatermarkStrategy
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDS.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            //生成自定义的watermark
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new Myperiod(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {//提取数据的时间戳
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        //4.按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(date -> date.getId());

        //5.开窗
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //6.计算总和
        window.sum("vc").print();


        env.execute();
    }

    //自定义周期性的Watermark生成器
    public static class Myperiod implements WatermarkGenerator<WaterSensor> {


        //最大时间戳
        private Long maxTs;

        //最大延迟时间
        private Long maxDelay;

        //构造方法
        public Myperiod(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + this.maxDelay + 1;
        }

        //当数据来的时候调用
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(eventTimestamp, maxTs);

        }

        //周期性调用
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

            System.out.println("生成Watermark");

            output.emitWatermark(new Watermark(maxTs - maxDelay - 1L));

        }
    }
}
