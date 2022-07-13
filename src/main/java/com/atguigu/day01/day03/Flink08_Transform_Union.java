package com.atguigu.day01.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2022/7/2 000217:14
 * @Version 1.0
 */
public class Flink08_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<Integer> source2 = env.fromElements(10, 20, 30, 40, 50);

        DataStreamSource<Integer> source3 = env.fromElements(100, 200, 300, 400, 500);

        source1.union(source2).union(source3).print("+"+"hhhhh");

        env.execute();
    }
}
