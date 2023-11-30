package com.yueng.chapter5_transform;

import com.yueng.chapter5_source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-09-26-18:45
 */
public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("Josh", "cart", 2000L),
                new Event("Silvia", "good_detail", 3000L),
                new Event("Mark", "payment", 4000L)
        );
        SingleOutputStreamOperator<String> filterMapResult = source.filter(data -> data.timestamp >= 3000L).map(data -> data.username);
        filterMapResult.print();
        env.execute();
    }
}
