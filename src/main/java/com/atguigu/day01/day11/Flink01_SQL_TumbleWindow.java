package com.atguigu.day01.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author cc
 * @create 2022/7/13 00139:30
 * @Version 1.0
 */
public class Flink01_SQL_TumbleWindow {
    public static void main(String[] args) {
        //todo  1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo  2.使用DDl 创建动态表  注意提时间RT/PT
        tableEnv.executeSql("" +
                "CREATE TABLE sensor_pt (  " +
                "  `id` STRING,  " +
                "  `ts` Bigint,  " +
                "  `vc` Double,  " +
                "  `pt` AS PROCTIME()  " +
                ") WITH (  " +
                "  'connector' = 'kafka',  " +
                "  'topic' = 'test',  " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',  " +
                "  'properties.group.id' = 'testGroup',  " +
                "  'scan.startup.mode' = 'latest-offset',  " +
                "  'format' = 'csv'  " +
                ")");

        tableEnv.executeSql("" +
                "CREATE TABLE sensor_rt (\n" +
                "  `id` STRING,\n" +
                "  `ts` Bigint,\n" +
                "  `vc` Double,\n" +
                "  `rt` as TO_TIMESTAMP_LTZ(ts,0),\n" +
                "  WATERMARK FOR rt AS rt - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");
        //todo  3.分组开窗聚合
        tableEnv.sqlQuery("" +
                "SELECT   " +
                "  id,   " +
                "  SUM(vc),   " +
                "  TUMBLE_START(pt, INTERVAL '10' SECOND) stt,   " +
                "  TUMBLE_END(pt, INTERVAL '10' SECOND) ent   " +
                "FROM sensor_pt   " +
                "GROUP BY   " +
                "  TUMBLE(pt, INTERVAL '10' SECOND),   " +
                "  id");
        
        tableEnv.sqlQuery("" +
                "SELECT   " +
                "  id,   " +
                "  SUM(vc),   " +
                "  TUMBLE_START(rt, INTERVAL '10' SECOND) stt,   " +
                "  TUMBLE_END(rt, INTERVAL '10' SECOND) ent   " +
                "FROM sensor_rt   " +
                "GROUP BY   " +
                "  TUMBLE(rt, INTERVAL '10' SECOND),   " +
                "  id").execute().print();

    }
}
