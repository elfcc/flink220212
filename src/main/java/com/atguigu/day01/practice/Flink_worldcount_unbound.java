package com.atguigu.day01.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2022/7/9 000923:49
 * @Version 1.0
 */
public class Flink_worldcount_unbound {
    public static void main(String[] args) throws Exception {
        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //数据来源
        DataStreamSource<String> source = env.socketTextStream("hadoop102",9999);
        //进行faltmap
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(Tuple2.of(s1, 1));
                }
            }
        });

        //进行分组keyby
        streamOperator.keyBy(0)
                .sum(1)
                .print();

        //
        env.execute();
    }
}
