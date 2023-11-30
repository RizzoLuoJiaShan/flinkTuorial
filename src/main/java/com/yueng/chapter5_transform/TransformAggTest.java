package com.yueng.chapter5_transform;

import com.yueng.chapter5_source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-09-26-19:41
 */
public class TransformAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("Josh", "cart", 2000L),
                new Event("Silvia", "home", 4500L),
                new Event("Mark", "payment", 4000L),
                new Event("Josh", "order", 3000L),
                new Event("Josh", "payment", 2500L),
                new Event("Josh", "home", 3600L)
        );
//        source.keyBy(data -> data.username).max("timestamp").print("max");
        source.keyBy(data -> data.username).maxBy("timestamp").print("maxBy");
        env.execute();
    }
}
