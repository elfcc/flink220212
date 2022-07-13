package com.atguigu.day01.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author cc
 * @create 2022/7/12 001211:17
 * @Version 1.0
 */
public class Flink04_KafakSource {
    public static void main(String[] args) {
        //todo 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo  读取卡法咖数据创建表
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        tableEnv.connect(new Kafka()
        .version("universal")
        .topic("test")
        .startFromLatest()
        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
        .property(ConsumerConfig.GROUP_ID_CONFIG,"220121")
        ).withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");
        //todo  简单查询表  打印
        Table sensor = tableEnv.from("sensor");
        sensor.where($("id").isEqual("sensor_2"))
                .execute().print();

    }
}
