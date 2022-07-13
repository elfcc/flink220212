package com.atguigu.day01.day04;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author cc
 * @create 2022/7/5 00052:14
 * @Version 1.0
 */
public class Flink06_Sink_JdBc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });
        SinkFunction<WaterSensor> sink = JdbcSink.sink("inset into ", new JdbcStatementBuilder<WaterSensor>() {
            @Override
            public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                preparedStatement.setString(1, waterSensor.getId());
                preparedStatement.setLong(2, waterSensor.getTs());
                preparedStatement.setInt(3, waterSensor.getVc());
            }
        }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(" ")
                .withDriverName("")
                .withPassword("123456")
                .withUsername("root")
                .build());
        map.addSink(sink);
        env.execute();

    }
}
