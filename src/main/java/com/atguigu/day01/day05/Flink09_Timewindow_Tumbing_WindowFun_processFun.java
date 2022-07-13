package com.atguigu.day01.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.shell.Count;

/**
 * @author cc
 * @create 2022/7/5 00059:18
 * @Version 1.0
 */
public class Flink09_Timewindow_Tumbing_WindowFun_processFun {
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


        //采用Windowfun 进行计算  计数
        SingleOutputStreamOperator<Integer> process = window.process(new ProcessWindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {
            private Integer count = 0;

            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Integer> out) throws Exception {
                System.out.println("process.....");
                for (Tuple2<String, Integer> element : elements) {
                    count++;
                }
                out.collect(count);
            }
        });

        process.print();

        env.execute();


    }
}



