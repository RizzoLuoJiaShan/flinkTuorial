package com.yueng.chapter6_window;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @author RizzoYueng
 * @create 2023-09-30-17:09
 */
public class WindowAggProcessFunction {
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
        // 增量聚合函数&全窗口函数联合调用
        watermarkedSource.keyBy(data -> true).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new UVAgg(), new UVPrintProcess()).print();
        env.execute();
    }
    // 自定义实现aggregate function，实现增量聚合计算UV值
    public static class UVAgg implements AggregateFunction<Event, HashSet<String>, Integer>{

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<String>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.username);
            return accumulator;
        }

        @Override
        public Integer getResult(HashSet<String> accumulator) {
            return accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            a.addAll(b);
            return a;
        }
    }

    // 自定义实现ProcessWindowFunction，主要就是为了实现包装窗口信息
    public static class UVPrintProcess extends ProcessWindowFunction<Integer, String, Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Integer, String, Boolean, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Integer uv = elements.iterator().next();
            out.collect("窗口在"+new Timestamp(start)+"~"+new Timestamp(end)+"的访问用户数为："+uv);
        }
    }
}
