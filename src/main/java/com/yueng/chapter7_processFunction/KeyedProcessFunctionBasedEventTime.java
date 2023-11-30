package com.yueng.chapter7_processFunction;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-10-01-15:33
 */
public class KeyedProcessFunctionBasedEventTime {
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
        source.keyBy(data -> data.username).process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                // 获取事件时间
                Long currentTs = ctx.timestamp();
                out.collect(ctx.getCurrentKey()+"数据的事件时间为"+new Timestamp(currentTs)+"当前水位线为"+ctx.timerService().currentWatermark());
                // 注册定时器，当前元素事件时间10S后的定时器
                ctx.timerService().registerEventTimeTimer(currentTs+10000L);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(ctx.getCurrentKey()+"定时器被触发了，触发的时间为"+new Timestamp(timestamp)+"当前水位线为"+ctx.timerService().currentWatermark());
            }
        }).print();
        env.execute();
    }
}
