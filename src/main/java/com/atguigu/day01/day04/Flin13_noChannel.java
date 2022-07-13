package com.atguigu.day01.day04;

import com.atguigu.day01.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author cc
 * @create 2022/7/5 000512:01
 * @Version 1.0
 */
public class Flin13_noChannel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1)
                .addSource(new AppMarketingDataSource())
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String ,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        return Tuple2.of(marketingUserBehavior.getBehavior(),1);
                    }
                }).keyBy(0)
                .sum(1)
                .print();
        env.execute();

    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}

