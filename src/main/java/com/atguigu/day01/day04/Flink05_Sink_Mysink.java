package com.atguigu.day01.day04;

import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author cc
 * @create 2022/7/4 000415:50
 * @Version 1.0
 */
public class Flink05_Sink_Mysink {
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

        map.addSink(new Mysink());

        env.execute();


    }
    public static class Mysink implements SinkFunction<WaterSensor>{
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            //1。创建连接
            Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?userSSL=false", "root", "123456");

            //语句执行者
            PreparedStatement pstm = connection.prepareStatement("insert into sensor value (?,?,?)");

            pstm.setNString(1,value.getId());
            pstm.setLong(2,value.getTs());
            pstm.setInt(3,value.getVc());

            pstm.execute();
            pstm.close();

            connection.close();





        }
    }
}