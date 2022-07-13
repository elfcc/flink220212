package com.atguigu.day01.day09;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;

import java.util.List;
import java.util.Map;

/**
 * @author cc
 * @create 2022/7/11 001114:01
 * @Version 1.0
 */
public class Flink0101 {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取数据来源
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        //TODO 3.转换为Javabean
        SingleOutputStreamOperator<WaterSensor> map = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
            }
        });
        SingleOutputStreamOperator<WaterSensor> watermarks = map
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                return waterSensor.getTs() * 1000;
            }
        }));
        //TODO 4.按照ID分组
        KeyedStream<WaterSensor, String> keyedStream = watermarks.keyBy(WaterSensor::getId);



        //TODO  5.定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start").where(new SimpleCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor) throws Exception {
                return waterSensor.getVc() > 30;
            }
        }).next("n1").where(new SimpleCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor) throws Exception {
                return waterSensor.getVc() > 30;
            }
        }).next("n2").where(new SimpleCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor) throws Exception {
                return waterSensor.getVc() > 30;
            }
        });
        //TODO  6.将模式作用在流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(keyedStream, pattern);
        //TODO  7.提取事件
        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                List<WaterSensor> start = map.get("start");
                System.out.println(start);
                System.out.println(map.get("n1"));
                System.out.println(map.get("n2"));

                WaterSensor waterSensor = start.get(0);
                return waterSensor.getId() + "连续三条水位线在30以上";

            }
        });

        //
        select.print(">>>>>>>>>>>");
        env.execute();
    }
}
