package com.atguigu.day01.day04;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author cc
 * @create 2022/7/4 00049:54
 * @Version 1.0
 */
public class Flin01_process {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });
        KeyedStream<WaterSensor, Tuple> keyBy = map.keyBy("id");

        SingleOutputStreamOperator<WaterSensor> process = keyBy.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            private Integer lastSumvc = 0;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                lastSumvc += value.getVc();
                out.collect(new WaterSensor(value.getId(), value.getTs(), lastSumvc));

            }
        });
        process.print();

        env.execute();
    }
}
