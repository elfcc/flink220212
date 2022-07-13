package com.atguigu.day01.day09;

import com.atguigu.day01.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author cc
 * @create 2022/7/11 001122:03
 * @Version 1.0
 */
public class Flink05_statge {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.数据来源
        DataStreamSource<String> source = env.readTextFile("input/LoginLog.csv");

        //TODO 3.对数据进行map装换城javabean
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
        //TODO 提取一下事件的时间 方便后面的next关系
        SingleOutputStreamOperator<LoginEvent> operator = map.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        return loginEvent.getEventTime() * 1000;
                    }
                }));
        //TODO 4.对数据进行分组userid
        KeyedStream<LoginEvent, Long> keyedStream = operator.keyBy(LoginEvent::getUserId);

        //
        SingleOutputStreamOperator<String> value = keyedStream.flatMap(new RichFlatMapFunction<LoginEvent, String>() {
            //定义状态
            private ValueState<LoginEvent> valueState;
            //初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("value", LoginEvent.class));
            }

            @Override
            public void flatMap(LoginEvent loginEvent, Collector<String> collector) throws Exception {
                //TODO  使用状态
                if ("fail".equals(loginEvent.getEventType())) {
                    LoginEvent lastvalue = valueState.value();
                    if (lastvalue != null && loginEvent.getEventTime() - lastvalue.getEventTime() <= 2) {

                        collector.collect(lastvalue.getUserId() + "在" + lastvalue.getEventTime() + "到" + loginEvent.getEventTime() + "连续登陆了两次失败");
                    }
                    valueState.update(loginEvent);

                } else {
                    valueState.clear();
                }
            }
        });

        //TODO 8.输出数据
        value.print(">>>>>>>>>>>>>>>>");
        //TODO 9.启动任务
        env.execute();



    }
}
