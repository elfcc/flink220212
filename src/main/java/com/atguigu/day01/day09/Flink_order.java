package com.atguigu.day01.day09;

import com.atguigu.day01.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @create 2022/7/12 00121:19
 * @Version 1.0
 */
public class Flink_order {
    public static void main(String[] args) throws Exception {
        //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //
        DataStreamSource<String> source = env.readTextFile("input/OrderLog.csv");

        SingleOutputStreamOperator<OrderEvent> map = source.map(line -> {
            String[] split = line.split(",");
            return new OrderEvent(Long.parseLong(split[0]),
                    split[1],
                    split[2],
                    Long.parseLong(split[3]));
        });
        SingleOutputStreamOperator<OrderEvent> streamOperator = map.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent orderEvent, long l) {
                        return orderEvent.getEventTime() * 1000L;
                    }
                }));
        KeyedStream<OrderEvent, Long> keyedStream = streamOperator.keyBy(OrderEvent::getOrderId);

        //定义模式规范
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
        })
                .within(Time.minutes(15));

        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);
        OutputTag<String> outputTag = new OutputTag<>("output");
        SingleOutputStreamOperator<Tuple2<OrderEvent, OrderEvent>> select = patternStream.select(outputTag, new PatternTimeoutFunction<OrderEvent, String>() {
            @Override
            public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                OrderEvent start = map.get("start").get(0);
                return start.getOrderId() + ":" + start.getEventType();
            }
        }, new PatternSelectFunction<OrderEvent, Tuple2<OrderEvent, OrderEvent>>() {
            @Override
            public Tuple2<OrderEvent, OrderEvent> select(Map<String, List<OrderEvent>> map) throws Exception {
                OrderEvent start = map.get("start").get(0);
                OrderEvent end = map.get("next").get(0);
                return new Tuple2<>(start, end);
            }
        });

        select.print(">>>>>>>>>>>>>>>");
        select.getSideOutput(outputTag).print("timout>>>>>>>>>>>");

        env.execute();
    }
}
