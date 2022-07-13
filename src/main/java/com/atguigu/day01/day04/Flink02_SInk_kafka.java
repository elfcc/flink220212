package com.atguigu.day01.day04;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.Serializable;

/**
 * @author cc
 * @create 2022/7/4 000411:27
 * @Version 1.0
 */
public class Flink02_SInk_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //转换成javabean 再转换成Json字符串
        SingleOutputStreamOperator<String> map = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] split = s.split(",");

                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

                return JSONObject.toJSONString(waterSensor);
            }
        });

        map.addSink(new FlinkKafkaProducer<String>("hadoop102:9092","flink", new SimpleStringSchema()));

        env.execute();


    }
}
