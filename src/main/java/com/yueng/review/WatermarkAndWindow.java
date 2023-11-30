package com.yueng.review;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author RizzoYueng
 * @create 2023-10-18-19:51
 */
public class WatermarkAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200L);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        source.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new MyAgg(), new MyProcessWindowFunc())
                .print();
        env.execute();
    }
    public static class MyAgg implements AggregateFunction<Event, Tuple2<String, Long>, Tuple2<String, Long>>{

        @Override
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of("", 0L);
        }

        @Override
        public Tuple2<String, Long> add(Event value, Tuple2<String, Long> accumulator) {
            Long times = accumulator.f1+1;
            return Tuple2.of(value.url, times);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
            return null;
        }
    }

    public static class MyProcessWindowFunc extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Tuple2<String, Long> urlRes = elements.iterator().next();
            out.collect(urlRes.f0+"在窗口"+new Timestamp(start)+"~~"+new Timestamp(end)+"之间的点击数为："+urlRes.f1);
        }
    }
}
