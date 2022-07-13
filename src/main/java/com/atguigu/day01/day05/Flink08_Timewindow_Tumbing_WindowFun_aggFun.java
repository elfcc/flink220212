package com.atguigu.day01.day05;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author cc
 * @create 2022/7/5 00059:18
 * @Version 1.0
 */
public class Flink08_Timewindow_Tumbing_WindowFun_aggFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转为Tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = streamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        //4.将相同的单词聚合到同一个分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);


        //TODO 5.开启一个基于时间的滚动窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));


        //采用Windowfun 进行计算
        SingleOutputStreamOperator<Integer> aggregate = window.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器");
                return 0;
            }

            @Override
            public Integer add(Tuple2<String, Integer> stringIntegerTuple2, Integer integer) {
                System.out.println("进行累加计算");
                return stringIntegerTuple2.f1 + integer;
            }

            @Override
            public Integer getResult(Integer integer) {
                System.out.println("获取累加器的结果");
                return integer;
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                return integer+acc1;
            }
        });

        aggregate.print();

        env.execute();


    }
}



