package com.atguigu.day01.day09;

import com.atguigu.day01.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author cc
 * @create 2022/7/12 00120:30
 * @Version 1.0
 */
public class Flink06 {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取数据
        DataStreamSource<String> source = env.readTextFile("input/OrderLog.csv");
        //TODO 3.将数据转换成javabean  提取事件时间
        SingleOutputStreamOperator<OrderEvent> map = source.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new OrderEvent(Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
            }
        });
        SingleOutputStreamOperator<OrderEvent> watermarks = map.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent orderEvent, long l) {
                        return orderEvent.getEventTime() * 1000;
                    }
                }));
        //TODO  4.按照订单ID分组
        KeyedStream<OrderEvent, Long> keyedStream = watermarks.keyBy(OrderEvent::getOrderId);
        //TODO  5.定义模式序列
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "create".equals(orderEvent.getEventType());
            }
        }).next("next").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "pay".equals(orderEvent.getEventType());
            }
        }).within(Time.minutes(15));

        //TODO 6.将模式作用到流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取事件（包含超时事件）
        OutputTag<String> output = new OutputTag<String>("outputtag"){};
        SingleOutputStreamOperator<Tuple2<OrderEvent, OrderEvent>> select = patternStream.select(output, new PatternTimeoutFunction<OrderEvent, String>() {
            @Override
            public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                OrderEvent start = map.get("start").get(0);
                return start.getOrderId() + ":" + start.getEventTime();

            }
        }, new PatternSelectFunction<OrderEvent, Tuple2<OrderEvent, OrderEvent>>() {
            @Override
            public Tuple2<OrderEvent, OrderEvent> select(Map<String, List<OrderEvent>> map) throws Exception {
                OrderEvent start = map.get("start").get(0);
                OrderEvent next = map.get("next").get(0);

                return new Tuple2<>(start, next);
            }
        });

        //TODO 8.打印数据
        select.getSideOutput(output).print(".》》》》》》》》》》");
        select.print(">>>>>>>>>>>");

        //TODO 9.启动任务
        env.execute();




    }
}
