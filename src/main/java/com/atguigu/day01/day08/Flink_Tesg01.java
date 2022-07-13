package com.atguigu.day01.day08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;

/**
 * @author cc
 * @create 2022/7/9 000914:16
 * @Version 1.0
 */
public class Flink_Tesg01 {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(2);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> map = source.map(line -> line).setParallelism(2);

        map.broadcast().print("broadcast").setParallelism(4);

        map.global().print().setParallelism(4);


    }
}
