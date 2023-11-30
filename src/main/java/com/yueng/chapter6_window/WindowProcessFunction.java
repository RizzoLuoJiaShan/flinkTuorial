package com.yueng.chapter6_window;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @create 2023-09-30-16:42
 */
public class WindowProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        SingleOutputStreamOperator<String> processed = source.keyBy(data -> true).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new CustomProcessFunction());
        processed.print();
        env.execute();
    }
    public static class CustomProcessFunction extends ProcessWindowFunction<Event, String, Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            // 将所有的窗口内的元素放在hashset中对用户名去重
            HashSet<String> names = new HashSet<>();
            for (Event element : elements) {
                names.add(element.username);
            }
            // 获取时间窗的起点和终点
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect("窗口"+new Timestamp(start)+"~~"+new Timestamp(end)+"的访问人数为："+names.size());
        }
    }
}
