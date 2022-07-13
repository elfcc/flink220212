package com.atguigu.day01.day10;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author cc
 * @create 2022/7/12 00129:44
 * @Version 1.0
 */
public class Flink12_Sql_Pt_toTable {
    public static void main(String[] args) {

        //todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //todo 1.1 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo 2.读取数据
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0],
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2])
            );
        });
        //todo 3.获取动态表
        Table table = tableEnv.fromDataStream(map,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());
        //todo 4.打印
        table.execute().print();


    }
}
