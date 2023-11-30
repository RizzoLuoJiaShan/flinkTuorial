package com.yueng.chapter6_window;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author RizzoYueng
 * @create 2023-09-30-15:53
 */
public class WindowAggregatePV_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> watermarkedSource = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        watermarkedSource.print("data: ");
        // 所有数据放在一起统计
        SingleOutputStreamOperator<Double> aggregated = watermarkedSource.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CustomAggregateFunction());
        aggregated.print();
        env.execute();
    }
    // 自定义一个aggregate的function
    public static class CustomAggregateFunction implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>{

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
            accumulator.f1.add(value.username);
            return Tuple2.of(accumulator.f0+1, accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
            return (double) accumulator.f0 / accumulator.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
            a.f1.addAll(b.f1);
            return Tuple2.of(a.f0+b.f0, a.f1);
        }
    }
}
