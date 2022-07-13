package com.atguigu.day01.day09;

import com.atguigu.day01.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @create 2022/7/12 00129:20
 * @Version 1.0
 */
public class Flink_Fail02 {
    public static void main(String[] args) throws Exception {
        //ToDo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //ToDo 2.读取数据
        DataStreamSource<String> source = env.readTextFile("input/");
        //ToDo 3.转换成javabean  提取事件时间
        SingleOutputStreamOperator<LoginEvent> map = source.map(line -> {
            String[] split = line.split(",");
            return new LoginEvent(Long.parseLong(split[0]),
                    split[1],
                    split[2],
                    Long.parseLong(split[3]));
        });
        SingleOutputStreamOperator<LoginEvent> streamOperator = map.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        return loginEvent.getEventTime() * 1000;
                    }
                }));
        //ToDo 4.按照userID分组
        KeyedStream<LoginEvent, Long> keyedStream = streamOperator.keyBy(LoginEvent::getUserId);
        //ToDo 5.定义模式序列
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) throws Exception {
                return "fail".equals(loginEvent.getEventType());
            }
        }).times(2)
                .within(Time.seconds(2));
        //ToDo 6.将模式序列作用在流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);
        //ToDo 7.提取事件
        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                List<LoginEvent> events = map.get("start");

                LoginEvent start = events.get(0);
                LoginEvent end = events.get(1);
                return start.getUserId() + ":" + start.getEventTime() + end.getEventTime();
            }
        });
        //ToDo 8.打印结果
        select.print(">>>>>>>>>>>>>>");
        //ToDo 9.启动任务
        env.execute();
    }
}