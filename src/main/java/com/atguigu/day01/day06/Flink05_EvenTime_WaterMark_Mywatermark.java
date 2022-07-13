package com.atguigu.day01.day06;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author cc
 * @create 2022/7/6 00069:25
 * @Version 1.0
 */
public class Flink05_EvenTime_WaterMark_Mywatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        //设置watermark
        SingleOutputStreamOperator<WaterSensor> watermarks = map.assignTimestampsAndWatermarks(
                WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                        return null;
                    }
                })
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor waterSensor, long l) {
                                return waterSensor.getTs() * 1000;
                            }
                        }));
        //keyby  聚合相同ID
        KeyedStream<WaterSensor, Tuple> id = watermarks.keyBy("id");


        //开启一个窗口  基于事件时间创建一个滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = id.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        
        window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + key
                        + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd()/1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        })
                .print();

env.execute();

    }
    public static class MyWaterMark implements WatermarkGenerator{
        @Override
        public void onEvent(Object o, long l, WatermarkOutput watermarkOutput) {

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

        }
    }
}
