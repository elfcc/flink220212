package com.atguigu.day01.day03;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2022/7/2 000216:51
 * @Version 1.0
 */
public class Flink06_Transform_Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> source = env.fromElements("1", "2", "3", "4");
        source.print("原来的分区是：");
        DataStream<String> shuffle = source.shuffle();
        shuffle.print("现在的分区是：");

        env.execute();
    }
}
