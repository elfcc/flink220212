package com.atguigu.day01.day03;

import akka.dispatch.sysmsg.Watch;
import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.file.Watchable;
import java.util.Random;

/**
 * @author cc
 * @create 2022/7/2 000211:47
 * @Version 1.0
 */
public class Flink0202 {
    public static void main(String[] args) throws Exception {
        // 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.addSource(new MySource());

        source.print();

        env.execute();
    }
    public static class MySource implements SourceFunction<WaterSensor>{
        private Random random=new Random();
        private Boolean isrunnin=true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (true){
                ctx.collect(new WaterSensor("flink"+random.nextInt(1000),System.currentTimeMillis(),random.nextInt(100)));


            }
        }

        @Override
        public void cancel() {
                isrunnin=false;
        }
    }
}
