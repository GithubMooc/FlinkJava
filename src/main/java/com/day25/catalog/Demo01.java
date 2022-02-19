package com.day25.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author Master
 * @Date 2022/2/19
 * @Time 23:43
 * @Name FlinkJava
 * <p>
 * Catalog 提供了元数据信息，例如数据库、表、分区、视图以及数据库或其他外部系统中存储的函数和信息。
 * 数据处理最关键的方面之一是管理元数据。元数据可以是临时的，例如临时表、或者通过 TableEnvironment 注册的UDF。元数据也可以是持久化的，例如 Hive Metastore 中的元数据。Catalog 提供了一个统一的API，用于管理元数据，并使其可以从 Table API 和 SQL 查询语句中来访问。
 * 前面用到Connector其实就是在使用Catalog。
 * <p>
 * Catalog类型
 * GenericInMemoryCatalog（临时的）
 * GenericInMemoryCatalog是基于内存实现的Catalog，所有元数据只在 session 的生命周期内可用。
 * JdbcCatalog
 * JdbcCatalog 使得用户可以将Flink通过JDBC协议连接到关系数据库。PostgresCatalog是当前实现的唯一一种JDBC Catalog。
 * HiveCatalog（用的最多）
 * HiveCatalog有两个用途：作为原生 Flink 元数据的持久化存储，以及作为读写现有Hive元数据的接口。 Flink的Hive 文档提供了有关设置HiveCatalog以及访问现有Hive元数据的详细信息。
 */
public class Demo01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Catalog 名字
        String name = "myhive";
        // 默认数据库
        String defaultDatabase = "flink_test";
        // hive配置文件的目录. 需要把hive-site.xml添加到该目录
        String hiveConfDir = "c:/conf";

        // 1. 创建HiveCatalog
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        // 2. 注册HiveCatalog
        tEnv.registerCatalog(name, hive);
        // 3. 把 HiveCatalog: myhive 作为当前session的catalog
        tEnv.useCatalog(name);
        tEnv.useDatabase("flink_test");

        //设置用户权限
        System.setProperty("HADOOP_USER_NAME", "atguigu");


        //指定SQL语法为Hive语法
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tEnv.sqlQuery("select * from stu").execute().print();

    }
}
