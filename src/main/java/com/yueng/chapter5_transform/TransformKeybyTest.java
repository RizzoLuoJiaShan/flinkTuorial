package com.yueng.chapter5_transform;

import com.yueng.chapter5_source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-09-26-19:26
 */
public class TransformKeybyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("Josh", "cart", 2000L),
                new Event("Silvia", "home", 3000L),
                new Event("Mark", "payment", 4000L)
        );
        // keyBy 分组
        KeyedStream<Event, String> keyedStream = source.keyBy(data -> data.url);
        env.execute();
    }
}
