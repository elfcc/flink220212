package com.atguigu.day01.day10;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.logical.DateType;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author cc
 * @create 2022/7/12 001210:48
 * @Version 1.0
 */
public class Flink03_Filesource {
    public static void main(String[] args) {
        //TOdo  1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TOdo  2.读取数据创建表
        Schema field = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(field)
                .createTemporaryTable("sensor");

        //TOdo  4.执行查询
        Table sensor = tableEnv.from("sensor");
        /*Table select = sensor.where($("id").isEqual("1002"))
                .select($("id"), $("vc"));

        select.execute().print();*/
        //TOdo  5.打印
        sensor.execute().print();

    }
}
