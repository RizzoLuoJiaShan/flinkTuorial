package com.yueng.chapter8_divideStream;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-10-02-19:58
 */
public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> orderStream = env.fromElements(
                Tuple2.of("Catalina", 1000L),
                Tuple2.of("Bob", 1000L),
                Tuple2.of("Josh", 2000L),
                Tuple2.of("Lisa", 3000L),
                Tuple2.of("Josh", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                })
        );
        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Catalina", "payment", 1000L),
                new Event("Catalina", "order", 2000L),
                new Event("Bob", "payment", 1000L),
                new Event("Bob", "detail", 2000L),
                new Event("Josh", "payment", 1000L),
                new Event("Lisa", "payment", 2000L),
                new Event("Josh", "detail", 6000L),
                new Event("Lisa", "cart", 3500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );
        // 将下单流与点击流做一个interval join
        orderStream.keyBy(data -> data.f0)
                .intervalJoin(clickStream.keyBy(data -> data.username))
                .between(Time.milliseconds(-2000), Time.milliseconds(3000))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Event right, ProcessJoinFunction<Tuple2<String, Long>, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left+"发生前后的点击过的页面为=>"+right.url);
                    }
                }).print();
        env.execute();
    }
}
