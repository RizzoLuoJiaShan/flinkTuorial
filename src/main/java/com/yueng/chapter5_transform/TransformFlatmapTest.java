package com.yueng.chapter5_transform;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author RizzoYueng
 * @create 2023-09-26-19:06
 */
public class TransformFlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("Josh", "cart", 2000L),
                new Event("Silvia", "detail", 3000L),
                new Event("Mark", "payment", 4000L)
        );
        SingleOutputStreamOperator<String> result = source.flatMap((Event data, Collector<String> out) -> {
            if(data.username.length() >= 5){
                out.collect(data.username);
            }
        }).returns(Types.STRING);
        result.print();
        env.execute();
    }
}
