package com.atguigu.day01.day04;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.day01.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2022/7/5 00052:28
 * @Version 1.0
 */
public class Flink09_pv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

        //转换为javabean
        SingleOutputStreamOperator<UserBehavior> map = source.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");
                return new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[1]),
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });
        map.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
            return  "pv".equals(userBehavior.getBehavior());
            }
        }).map(new MapFunction<UserBehavior, Tuple2<String ,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                return Tuple2.of("pv",1);
            }
        }).keyBy(0)
                .sum(1)
                .print();

        env.execute();

    }
}
