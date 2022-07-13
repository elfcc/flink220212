package com.atguigu.day01.day03;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2022/7/2 000212:11
 * @Version 1.0
 */
public class Flink0302 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> source = env.readTextFile("input/Water");

        SingleOutputStreamOperator<WaterSensor> map = source.map(new MyMap());
        map.print();

        env.execute();
    }
    public static class MyMap extends RichMapFunction<String, WaterSensor> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public WaterSensor map(String s) throws Exception {
            return null;
        }
    }
}
