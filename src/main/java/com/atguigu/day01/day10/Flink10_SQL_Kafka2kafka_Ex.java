package com.atguigu.day01.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author cc
 * @create 2022/7/12 001214:29
 * @Version 1.0
 */
public class Flink10_SQL_Kafka2kafka_Ex {
    public static void main(String[] args) throws Exception {
        //todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 2.使用DDl 创建kafka source
        tableEnv.executeSql("" +
                "CREATE TABLE sensor1 (  " +
                "  `id` STRING,  " +
                "  `ts` Bigint,  " +
                "  `vc` Double   " +
                "    " +
                ") WITH (  " +
                "  'connector' = 'kafka',  " +
                "  'topic' = 'test',  " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',  " +
                "  'properties.group.id' = 'testGroup',  " +
                "  'scan.startup.mode' = 'latest-offset',  " +
                "  'format' = 'csv'  " +
                ")");
        //todo 3.读取kafak 的数据转换为流打印测试
        Table resultTable = tableEnv.sqlQuery("select * from sensor1 where id >='1002'");
        tableEnv.createTemporaryView("result1",resultTable);
        tableEnv.toDataStream(resultTable).print("》》》》》");
        //todo 4.使用ddl 的方式创建卡法咖的sink
        tableEnv.executeSql("" +
                "CREATE TABLE sensor2 (\n" +
                "  `id` STRING,\n" +
                "  `vc` Double \n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'test1',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");

        //todo 讲述据写出
        tableEnv.executeSql("insert into sensor2 select * from result1");
    }
}
