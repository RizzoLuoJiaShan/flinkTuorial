package com.yueng.chapter6_window;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-09-30-11:14
 */
public class WindowAndReduceFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarkedSource = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })).map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.username, 1L);
            }
        });
        /**
         * 滚动时间窗口的of方法，除了可以传入窗口大小之外，还可以传入一个offset
         * 这个offset的作用是用于调整时差，例如我们以一天作为窗口大小，但是默认是以UTC时间为标准
         * 而北京时间要比UTC时间早八小时，所以需要在offset中填入Time.hours(-8)
         */
        // watermarkedSource.keyBy(data -> data.f0).window(TumblingEventTimeWindows.of(Time.hours(1))); // 滚动 事件时间 窗口
        // watermarkedSource.keyBy(data -> data.f0).window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5))); // 滑动 事件时间 窗口，需要传入窗口大小和滑动步长两个参数
        // watermarkedSource.keyBy(data -> data.f0).window(EventTimeSessionWindows.withGap(Time.seconds(2))); // 事件时间 会话窗口，需要传入一个会话最长等待中止时间
        // watermarkedSource.keyBy(data -> data.f0).countWindow(10, 2); // 计数窗口，只填size参数就是滚动计数窗口，填size和slide两个参数就是滑动计数窗口

        // 定义窗口类型(滚动事件时间窗口)+窗口内调用的窗口函数
        SingleOutputStreamOperator<Tuple2<String, Long>> windowReduced = watermarkedSource.keyBy(data -> data.f0).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });
        windowReduced.print();
        env.execute();
    }
}
