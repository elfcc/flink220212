package com.atguigu.day01.day10;


import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author cc
 * @create 2022/7/12 00129:44
 * @Version 1.0
 */
public class Flink02_TableToDs {
    public static void main(String[] args) throws Exception {

        //todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(1);
        //todo 1.1 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 2.读取数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(
                    split[0],
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2])
            );
        });


        //todo 3.获取动态表
        Table table = tableEnv.fromDataStream(map);

        //todo 4。简单查询
        Table select = table.where($("id").isEqual("1001"))
                //.groupBy($("id"))
                //.aggregate($("vc").sum().as("vc"))
                .select($("id"), $("vc"));
        //todo 5.对表直接打印
        //todo 6.J将表转换为流打印
        tableEnv.toDataStream(select).print("toDataStream>>>>>");
        tableEnv.toAppendStream(select,Row.class).print("toAppendStream>>>>");
        tableEnv.toChangelogStream(select).print("toChangelogStream>>>>>>");
        tableEnv.toRetractStream(select, Row.class).print("toRetractStream>>>>>>>>>");
        //todo 7.启动任务
        env.execute();
    }
}
