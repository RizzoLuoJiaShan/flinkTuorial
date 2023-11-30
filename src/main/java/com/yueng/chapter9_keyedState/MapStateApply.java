package com.yueng.chapter9_keyedState;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author RizzoYueng
 * @create 2023-10-04-14:51
 */
public class MapStateApply {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 按照url分组，实现滚动窗口统计url点击的次数
        source.keyBy(data -> data.url)
                .process(new FakeTumblingWindow(10000L))
                .print();

        env.execute();
    }

    public static class FakeTumblingWindow extends KeyedProcessFunction<String, Event, String> {
        private Long windowRange;

        public FakeTumblingWindow(Long windowRange) {
            this.windowRange = windowRange;
        }

        // 用于保存每个窗口中统计的count值
        private MapState<Long, Integer> windowUrlCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Integer>("window-count", Types.LONG, Types.INT));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            long windowStart = value.timestamp / windowRange * windowRange;
            long windowEnd = windowStart + windowRange;
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);
            // 如果已经有了这个窗口，就在之前的基础上+1
            if (windowUrlCountMapState.contains(windowStart)){
                Integer count = windowUrlCountMapState.get(windowStart);
                windowUrlCountMapState.put(windowStart, count+1);
            }else{
                // 如果没有这个窗口就创建一个新的记录
                windowUrlCountMapState.put(windowStart, 1);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey()+"在窗口"+new Timestamp(timestamp+1-windowRange)
                    +"~"+new Timestamp(timestamp + 1)
                    +"内的访问量为："
                    +windowUrlCountMapState.get(timestamp+1-windowRange));
            // 定时器触发，内容输出，窗口关闭清除
            windowUrlCountMapState.remove(timestamp+1-windowRange);
        }
    }
}
