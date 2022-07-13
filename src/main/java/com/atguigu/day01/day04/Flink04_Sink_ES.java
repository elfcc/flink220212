package com.atguigu.day01.day04;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.day01.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;

import java.util.ArrayList;

/**
 * @author cc
 * @create 2022/7/4 000414:41
 * @Version 1.0
 */
public class Flink04_Sink_ES {
    public static <waterSensor> void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //数据来源
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        //转换成json 类处理数据
        SingleOutputStreamOperator<WaterSensor> map = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });
        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        map.addSink(new ElasticsearchSink.Builder<WaterSensor>(httpHosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                IndexRequest indexRequest = new IndexRequest("0212","_doc","1001");
                requestIndexer.add(indexRequest);
            }
        }).build());


    }
}
