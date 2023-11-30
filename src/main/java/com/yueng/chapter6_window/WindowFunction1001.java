package com.yueng.chapter6_window;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.util.Times;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-10-01-11:04
 */
public class WindowFunction1001 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        // 获取数据源
        DataStreamSource<Event> source = env.addSource(new ClickSource());
        // 配置水位线
        SingleOutputStreamOperator<Event> watermarkedSource = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );
        watermarkedSource.print("data");
        // 先keyBy再window再配置窗口计算API
        watermarkedSource.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) //配置窗口大小为10S，类型为滚动事件时间窗口
                .aggregate(new PVAgg(), new PVPrintProcess()).print();
        // 启动环境
        env.execute();
    }
    public static class PVAgg implements AggregateFunction<Event, Integer, Integer>{

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

    public static class PVPrintProcess extends ProcessWindowFunction<Integer, String, Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Integer, String, Boolean, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Integer pvCount = elements.iterator().next();
            out.collect("窗口为"+new Timestamp(start)+"~"+new Timestamp(end)+"的总访问次数为："+pvCount);
        }
    }
}
