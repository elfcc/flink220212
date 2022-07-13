package com.atguigu.day01.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author cc
 * @create 2022/7/5 00059:25
 * @Version 1.0
 */
public class Flink02_Windoe_Sliding {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1)
                .socketTextStream("hadoop102",9999)
                .map(new MapFunction<String, Tuple2<String ,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s,1);
                    }
                }).keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(6),Time.seconds(3)));
    }
}
