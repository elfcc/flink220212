package com.atguigu.day01.day07;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class Flink05_aggState {
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

        //TODO 求平均值
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String >() {


            //1.定义状态

            private AggregatingState<Integer,Double> aggregatingState;
            @Override
                public void open(Configuration parameters) throws Exception {
                //2.初始化状态
                aggregatingState=getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>("agg", new AggregateFunction<Integer, Tuple2<Integer,Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> integerIntegerTuple2) {
                        return Tuple2.of(integerIntegerTuple2.f0 + integer, integerIntegerTuple2.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {
                        return integerIntegerTuple2.f0 * 1D / integerIntegerTuple2.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                        return Tuple2.of(integerIntegerTuple2.f0 + acc1.f0, integerIntegerTuple2.f1 + acc1.f0);
                    }
                },Types.TUPLE(Types.INT,Types.DOUBLE)));


    }
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                aggregatingState.add(value.getVc());

                Double aDouble = aggregatingState.get();

                out.collect(aDouble+"");
            }
}).print();
        env.execute();
    }
}
