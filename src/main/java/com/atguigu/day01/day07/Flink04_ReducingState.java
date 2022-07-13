package com.atguigu.day01.day07;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
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
 * @create 2022/7/8 000822:08
 * @Version 1.0
 */
public class Flink04_ReducingState {
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

        //TODO 进行两次数据流的求和
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String >() {
            //1.定义状态
            private ReducingState<Integer> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //2.初始化状态
                reducingState=getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reducing", new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer integer, Integer t1) throws Exception {
                        return integer+t1;
                    }
                }, Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //使用状态
                reducingState.add(value.getVc());


                Integer sumvc = reducingState.get();

                out.collect(sumvc+"");

            }
        }).print();
        env.execute();
    }
}
