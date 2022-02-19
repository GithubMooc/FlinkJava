package com.day10.state;

import org.apache.flink.contrib.streaming.state.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @Author Master
 * @Date 2022/2/13
 * @Time 02:14
 * @Name FlinkJava
 *
 * 可以在代码中单独为这个Job设置状态后端.
 */
public class Demo09 {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new HashMapStateBackend());
 		env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://checkpoints");
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoints/fs"));
//        存入本地的RocksDB数据库中
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
  		env.getCheckpointConfig().setCheckpointStorage("hdfs://checkpoints");
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/checkpoints/rocksdb"));
    }
}
