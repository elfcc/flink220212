package com.atguigu.day01.day07;


import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author cc
 * @create 2022/7/8 000821:13
 * @Version 1.0
 */
public class Flink01_ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //数据来源
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //将数据转换成waterjson le
        SingleOutputStreamOperator<WaterSensor> map = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");

                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //将数据分组
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(r -> r.getId());

        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String >() {
            //todo  1.定义一个状态
            private ValueState<Integer> valueState;
            @Override
            public void open(Configuration parameters) throws Exception {
                //todo 2.初始化状态
                valueState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value",Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //TODO 3.使用状态
                Integer lastvc =valueState==null?value.getVc():valueState.value();
                //
                //
                if (Math.abs(value.getVc()-lastvc)>10){
                    System.out.println("报警？？？？？？ 超过10");
                }
                //
            }
        }).print();

        env.execute();

    }
}

