package com.atguigu.day01.day10;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author cc
 * @create 2022/7/12 001214:13
 * @Version 1.0
 */
public class Flink08_SQl02 {
    public static void main(String[] args) {
        //todo   1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo   2.读取数据转换成动态表
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> map = source.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0],
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]));
        });
        Table table = tableEnv.fromDataStream(map);
        //todo   3.进行简单 的查询打印 注册的动态表
        tableEnv.createTemporaryView("sensor",table);
        //Table result = tableEnv.sqlQuery("select id ,count(*) vc ,sum(vc) from " + table + " group by id");
        Table result = tableEnv.sqlQuery(" select * from sensor where id >= '1002' ");
        //Table result = tableEnv.sqlQuery("select id ,count(*) vc ,sum(vc) from  sensor  group by id");
        //todo  4.输出打印结果
        result.execute().print();
    }
}
