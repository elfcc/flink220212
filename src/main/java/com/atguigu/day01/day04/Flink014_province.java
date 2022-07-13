package com.atguigu.day01.day04;

import com.atguigu.day01.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cc
 * @create 2022/7/5 000512:10
 * @Version 1.0
 */
public class Flink014_province {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1)
                .readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, Tuple2<String ,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        AdsClickLog adsClickLog = new AdsClickLog(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                split[2],
                                split[3],
                                Long.parseLong(split[4])
                        );
                        return Tuple2.of(adsClickLog.getProvince()+adsClickLog.getAdId(),1);
                    }
                }).keyBy(0)
                .sum(1)
                .print();
        env.execute();
    }
}
