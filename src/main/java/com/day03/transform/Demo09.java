package com.day03.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 23:05
 * @Name FlinkJava
 *
 * transform:KeySelector
 * 匿名内部类
 *
 * TODO 第一种方式： 指定 位置索引,只能用于 Tuple 的数据类型
 * KeyedStream<WaterSensor, Tuple> sensorKS = sensorDS.keyBy(0);
 * TODO 第二种方式：指定 字段名字,适用于 POJO
 * KeyedStream<WaterSensor, Tuple> sensorKS = sensorDS.keyBy("id");
 * TODO 第三种方式（推荐）：使用 KeySelector
 * KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
 *             @Override
 *             public String getKey(WaterSensor value) throws Exception {
 *                 return value.getId();
 *             }
 *         });
 */
public class Demo09 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 奇数分一组, 偶数分一组
        env
                .fromElements(10, 3, 5, 9, 20, 8)
                .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return value % 2 == 0 ? "偶数" : "奇数";
                    }
                })
                .print();
        env.execute();
    }
}
