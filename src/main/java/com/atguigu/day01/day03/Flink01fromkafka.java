package com.atguigu.day01.day03;

import akka.stream.scaladsl.PartitionHub;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import sun.security.krb5.Config;

import java.util.Properties;

/**
 * @author cc
 * @create 2022/7/2 000210:29
 * @Version 1.0
 */
public class Flink01fromkafka {
    public static void main(String[] args) throws Exception {
       //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从卡法ka读取数据
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers("hadoop102:9092")
//                .setGroupId("fghjk")
//                .setTopics("flink")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//
//        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "ghjk");


        //
        KafkaSource<String> stringKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("flink")
                .setGroupId("hjkl")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> source = env.fromSource(stringKafkaSource, WatermarkStrategy.noWatermarks(), "hjkl");

        source.print();

        env.execute();

    }
}
