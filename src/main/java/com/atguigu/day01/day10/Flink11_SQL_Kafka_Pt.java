package com.atguigu.day01.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author cc
 * @create 2022/7/12 001214:29
 * @Version 1.0
 */
public class Flink11_SQL_Kafka_Pt {
    public static void main(String[] args) throws Exception {
        //todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 2.使用DDl 创建kafka source
        tableEnv.executeSql("" +
                "CREATE TABLE sensor1 (\n" +
                "  `id` STRING,\n" +
                "  `ts` Bigint,\n" +
                "  `vc` Double,\n" +
                "  `pt` AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");
        //todo 3.就数据查询写出
        tableEnv.sqlQuery("select * from sensor1").execute().print();
    }
}
