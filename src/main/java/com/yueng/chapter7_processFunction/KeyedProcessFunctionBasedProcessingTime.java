package com.yueng.chapter7_processFunction;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author RizzoYueng
 * @create 2023-10-01-15:21
 */
public class KeyedProcessFunctionBasedProcessingTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.addSource(new ClickSource());
        source.keyBy(data -> data.username).process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                // 获取当前的处理时间
                long currentTs = ctx.timerService().currentProcessingTime();
                out.collect(ctx.getCurrentKey()+"数据处理时间为："+new Timestamp(currentTs));
                // 注册一个当前元素处理时间十秒后的定时器
                ctx.timerService().registerProcessingTimeTimer(currentTs+10000L);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(ctx.getCurrentKey()+"定时器被触发，触发时间："+new Timestamp(timestamp));
            }
        }).print();
        env.execute();
    }
}
