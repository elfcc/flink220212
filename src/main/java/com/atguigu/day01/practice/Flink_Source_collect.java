package com.atguigu.day01.practice;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author cc
 * @create 2022/7/10 001023:21
 * @Version 1.0
 */
public class Flink_Source_collect {
    public static void main(String[] args) throws Exception {
        //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6);
        source.print();

        env.execute();
    }
}
