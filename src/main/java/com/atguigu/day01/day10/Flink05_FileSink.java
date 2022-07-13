package com.atguigu.day01.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author cc
 * @create 2022/7/12 001211:25
 * @Version 1.0
 */
public class Flink05_FileSink {
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
        Table where = tableEnv.from("sensor").where($("id").isGreaterOrEqual("sensor_2"));
        //从这个表中查询到数据写出到
        tableEnv.connect(new FileSystem().path("input/sensor-sql2.txt"))
                .withFormat(new Csv().fieldDelimiter('|'))
                .withSchema(field)
                .createTemporaryTable("sensor2");
        where.executeInsert("sensor2");
    }
}