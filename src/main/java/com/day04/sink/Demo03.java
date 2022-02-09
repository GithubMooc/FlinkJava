package com.day04.sink;

import com.alibaba.fastjson.JSON;
import com.pojo.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.*;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.*;

/**
 * @Author Master
 * @Date 2022/2/10
 * @Time 00:00
 * @Name FlinkJava
 *  Sink到Elasticsearch
 *     如果是无界流, 需要配置bulk的缓存 注意：生产中不要这样设置为1
 *      esSinkBuilder.setBulkFlushMaxActions(1);
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        List<HttpHost> esHosts = Arrays.asList(
                new HttpHost("hadoop102", 9200),
                new HttpHost("hadoop103", 9200),
                new HttpHost("hadoop104", 9200));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env
                .fromCollection(waterSensors)
                .addSink(new ElasticsearchSink.Builder<WaterSensor>(esHosts, new ElasticsearchSinkFunction<WaterSensor>() {

                    @Override
                    public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                        // 1. 创建es写入请求
                        IndexRequest request = Requests
                                .indexRequest("sensor")
                                .id(element.getId())
                                .source(JSON.toJSONString(element), XContentType.JSON);
                        // 2. 写入到es
                        indexer.add(request);
                    }
                }).build());

        env.execute();
    }
}
