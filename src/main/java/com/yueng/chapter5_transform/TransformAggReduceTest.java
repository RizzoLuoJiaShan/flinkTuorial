package com.yueng.chapter5_transform;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-09-26-19:59
 */
public class TransformAggReduceTest {
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
        // 访问量最大的用户: 1、统计每个用户的访问频次；2、针对用户的访问频次求最大值
        SingleOutputStreamOperator<Tuple2<String, Long>> mapped = source.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.username, 1L);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> reduced = mapped.keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });
        reduced.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1 > value2.f1?Tuple2.of(value1.f0, value1.f1):Tuple2.of(value2.f0, value2.f1);
            }
        }).print("当前的最大访问次数的用户是：");
        env.execute();
    }
}
