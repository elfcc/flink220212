package com.atguigu.day01.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author cc
 * @create 2022/7/12 001214:29
 * @Version 1.0
 */
public class Flink09_SQL_Kafka2kafka {
    public static void main(String[] args) throws Exception {
        //todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo 2.使用ddl de 方式创建kafkasource
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
                "CREATE TABLE sensor2 (   " +
                "  `id` STRING,   " +
                "  `ts` Bigint,   " +
                "  `vc` Double    " +
                "     " +
                ") WITH (   " +
                "  'connector' = 'kafka',   " +
                "  'topic' = 'test1',   " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',   " +
                "  'format' = 'json'   " +
                ")");
        //todo 5.将数据写出
        tableEnv.executeSql("insert into sensor2 select * from result1");

        //todo 6.启动任务
        env.execute();
    }
}
