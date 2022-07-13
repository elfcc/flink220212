package com.atguigu.day01.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author cc
 * @create 2022/7/13 001311:57
 * @Version 1.0
 */
public class test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("" +
                "CREATE TABLE sensor_rt (\n" +
                "  `id` STRING,\n" +
                "  `ts` Bigint,\n" +
                "  `vc` Double,\n" +
                "  `rt` as TO_TIMESTAMP_LTZ(0),\n" +
                "  WATERMARK FOR rt AS rt - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");
        tableEnv.sqlQuery("selec * from sensor_rt").execute().print();
    }
}
