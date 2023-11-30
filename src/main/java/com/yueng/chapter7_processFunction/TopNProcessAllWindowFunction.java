package com.yueng.chapter7_processFunction;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * @author RizzoYueng
 * @create 2023-10-01-19:02
 */
public class TopNProcessAllWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        source.print("data->");
        // 不按照URL分组再求count，而是直接在时间窗内收集所有的URL，而在处理的时候使用HashMap键值对的形式保存每个URL出现的次数
        source.map(data -> data.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult())
                .print();
        env.execute();
    }

    public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Integer>, ArrayList<Tuple2<String, Integer>>> {

        @Override
        public HashMap<String, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Integer> add(String value, HashMap<String, Integer> accumulator) {
            if (accumulator.containsKey(value)) {
                Integer count = accumulator.get(value)+1;
                accumulator.put(value, count);
            } else {
                accumulator.put(value, 1);
            }
            return accumulator;
        }

        @Override
        public ArrayList<Tuple2<String, Integer>> getResult(HashMap<String, Integer> accumulator) {
            ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : accumulator.entrySet()) {
                String key = entry.getKey();
                Integer value = entry.getValue();
                list.add(Tuple2.of(key, value));
            }
            list.sort(new Comparator<Tuple2<String, Integer>>() {
                @Override
                public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                    return o2.f1 - o1.f1;
                }
            });
            return list;
        }

        @Override
        public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {
            return null;
        }
    }

    public static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Integer>>, String, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Integer>>, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Integer>>> elements, Collector<String> out) throws Exception {
            ArrayList<Tuple2<String, Integer>> list = elements.iterator().next();
            StringBuffer sb = new StringBuffer();
            long start = context.window().getStart();
            long end = context.window().getEnd();
            sb.append("--------------------------窗口" + new Timestamp(start) + "~~" + new Timestamp(end) + "----------------------------------" + "\n");
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Integer> tuple = list.get(i);
                String result = "No." + (i + 1) + "访问量的URL是" + tuple.f0 + "，总访问量为：" + tuple.f1 + "\n";
                sb.append(result);
            }
            out.collect(new String(sb));
        }
    }
}
