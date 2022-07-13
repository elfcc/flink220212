package com.atguigu.day01.day04;

import com.atguigu.day01.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.HashSet;


/**
 * @author cc
 * @create 2022/7/5 00058:53
 * @Version 1.0
 */
public class Flink11_UV_02 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1)
                .readTextFile("input/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] split = s.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4])
                        );
                        if ("pv".equals(userBehavior.getBehavior())){
                        collector.collect(Tuple2.of("uv",userBehavior.getUserId()));
                        }
                    }
                    })
                .keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Integer>>() {
                    private     HashSet<Long> uids = new HashSet<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                        uids.add(value.f1);
                        int size = uids.size();
                        out.collect(Tuple2.of("uv",size));

                    }
                }).print();
                }



}
