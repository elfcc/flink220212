package com.atguigu.day01.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author cc
 * @create 2022/7/13 00138:50
 * @Version 1.0
 */
public class Flink14_RTDDL {
    public static void main(String[] args) {
        //todo 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo  使用ddl 创建kafka source
        tableEnv.executeSql("" +
                "CREATE TABLE sensor1 ( " +
                "  `id` STRING, " +
                "  `ts` Bigint, " +
                "  `vc` Double, " +
                "  `rt` as TO_TIMESTAMP_LTZ(ts,0), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '5' SECOND " +
                ") WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = 'test', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'properties.group.id' = 'testGroup', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = 'csv' " +
                ")");
        //todo  打印
        tableEnv.sqlQuery("select * from sensor1").execute().print();
    }
}
