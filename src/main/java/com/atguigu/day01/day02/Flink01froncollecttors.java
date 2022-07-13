package com.atguigu.day01.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author cc
 * @create 2022/7/1 000116:36
 * @Version 1.0
 */
public class Flink01froncollecttors {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);

        DataStreamSource<Integer> source = env.fromCollection(list);

        source.print();

        env.execute();

    }
}
