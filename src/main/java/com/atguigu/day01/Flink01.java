package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import static java.lang.System.out;

/**
 * @author cc
 * @create 2022/6/29 002911:18
 * @Version 1.0
 */
public class Flink01 {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataSource<String> source = env.readTextFile("input/world.txt");

        //对数据进行flatmaop  de切分处理
        FlatMapOperator<String, String> flatMap = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        //对数据进行map 处理
        MapOperator<String, Tuple2<String, Integer>> mapvalue = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        //对map处理之后的数据进行reduce by
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = mapvalue.groupBy(0);
        //
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //
        result.print();
    }
}
