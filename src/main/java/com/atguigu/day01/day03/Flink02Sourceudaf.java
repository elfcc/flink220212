package com.atguigu.day01.day03;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author cc
 * @create 2022/7/2 000210:33
 * @Version 1.0
 */
public class Flink02Sourceudaf {
    public static void main(String[] args) throws Exception {
        //流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.addSource(new Mysource());

        source.print();

        env.execute();
    }

    public static class Mysource implements SourceFunction<WaterSensor>{
        private Random random=new Random();
        private Boolean isrunning=true;
        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            while (isrunning){
                sourceContext.collect(new WaterSensor("flink"+random.nextInt(1000),System.currentTimeMillis(),random.nextInt(100)));

            }

        }

        @Override
        public void cancel() {
            isrunning=false;
        }
    }
}
