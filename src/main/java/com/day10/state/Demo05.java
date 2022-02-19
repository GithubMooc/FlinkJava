package com.day10.state;

import com.pojo.WaterSensor;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2022/2/13
 * @Time 01:44
 * @Name FlinkJava
 * <p>
 * State:Managed State:Keyed State:ValueState<<UK, UV>>
 * 存储键值对列表.
 * 添加键值对:  put(UK, UV) or putAll(Map<UK, UV>)
 * 根据key获取值: get(UK)
 * 获取所有: entries(), keys() and values()
 * 检测是否为空: isEmpty()
 *
 * 去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
 */
public class Demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(3);
        env
                .socketTextStream("localhost", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    //定义状态 map中的key为水位因为要对相同的水位做去重，value为具体的数据
                    private MapState<Integer, WaterSensor> mapState;

                    //在声明周期open方法中初始化状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("map-state", Integer.class, WaterSensor.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        //判断如果mapState中不包含相同的水位则输出
                        if (!mapState.contains(value.getVc())) {
                            out.collect(value);
                            //更新状态
                            mapState.put(value.getVc(), value);
                        }
                    }
                })
                .print();
        env.execute();
    }
}


