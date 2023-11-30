package com.yueng.chapter6_lateData;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.util.Times;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-10-01-13:39
 */
public class WindowLateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> source = env.socketTextStream("hadoop102",7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] columns = value.split(",");
                        return new Event(columns[0], columns[1], Long.parseLong(columns[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 定义一个输出标签
        OutputTag<Event> outputTag = new OutputTag<Event>("late"){};

        SingleOutputStreamOperator<String> result = source.keyBy(data -> data.username)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .aggregate(new UserAgg(), new UserTimesProcess());

        result.print("result:");
        result.getSideOutput(outputTag).print("late");
        env.execute();
    }
    public static class UserAgg implements AggregateFunction<Event, Integer, Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Event value, Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a+b;
        }
    }
    public static class UserTimesProcess extends ProcessWindowFunction<Integer, String, String, TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<Integer, String, String, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Integer count = elements.iterator().next();
            out.collect(s + "在窗口"+new Timestamp(start)+"~"+new Timestamp(end)+"内的访问次数为："+count);
        }
    }
}
