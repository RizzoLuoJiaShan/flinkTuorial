package com.yueng.chapter6_window;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-10-01-11:23
 */
public class WindowURLCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );
        source.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new URLAgg(), new UrlPrintProcess())
                .print();
        env.execute();
    }

    public static class URLAgg implements AggregateFunction<Event, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return Tuple2.of(new String(), 0);
        }

        @Override
        public Tuple2<String, Integer> add(Event value, Tuple2<String, Integer> accumulator) {
            return Tuple2.of(value.url, accumulator.f1 + 1);
        }

        @Override
        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
            return Tuple2.of(a.f0, a.f1 + b.f1);
        }
    }

    public static class UrlPrintProcess extends ProcessWindowFunction<Tuple2<String, Integer>, UrlCountView, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, UrlCountView, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<UrlCountView> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Tuple2<String, Integer> next = elements.iterator().next();
            String url = next.f0;
            Integer count = next.f1;
            UrlCountView urlCountView = new UrlCountView(url, count, start, end);
            out.collect(urlCountView);
        }
    }
}
