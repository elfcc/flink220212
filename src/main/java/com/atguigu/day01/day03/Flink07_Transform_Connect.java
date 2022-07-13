package com.atguigu.day01.day03;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2022/7/2 000216:56
 * @Version 1.0
 */
public class Flink07_Transform_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> datasource = env.fromElements("a", "b", "c", "d");

        DataStreamSource<Integer> intersource = env.fromElements(1, 2, 3, 4);

        ConnectedStreams<String, Integer> connect = datasource.connect(intersource);

        connect.getFirstInput().print("第一次");
        connect.getSecondInput().print("第二次");

        env.execute();
    }
}
