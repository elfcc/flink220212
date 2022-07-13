package com.atguigu.day01.day09;

import com.atguigu.day01.bean.LoginEvent;
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
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @author cc
 * @create 2022/7/12 00122:12
 * @Version 1.0
 */
public class Flink_fail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/");

        SingleOutputStreamOperator<LoginEvent> map = source.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new LoginEvent(Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
            }
        });
        SingleOutputStreamOperator<LoginEvent> streamOperator = map.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        return loginEvent.getEventTime() * 1000L;
                    }
                }));
        KeyedStream<LoginEvent, Long> keyedStream = streamOperator.keyBy(LoginEvent::getUserId);

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).times(2)
                .within(Time.seconds(2));
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<String> start = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                List<LoginEvent> events = map.get("start");
                LoginEvent start = events.get(0);
                LoginEvent end = events.get(1);
                return start.getUserId() + ":" + start.getEventTime() + end.getEventTime();
            }
        });
        start.print(">>>>>>>>>>>>>>");
        env.execute();
    }
}
