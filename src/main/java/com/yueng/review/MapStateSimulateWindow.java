package com.yueng.review;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author RizzoYueng
 * @create 2023-10-22-17:41
 */
public class MapStateSimulateWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.keyBy(data -> data.url)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private Long windowSize = 10000L;
                    private MapState<Long, Long> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("mapWindow", Long.class, Long.class));
                    }

                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        // 先判断属于哪一个窗口
                        long windowStart = value.timestamp / windowSize * windowSize;
                        long windowEnd = windowStart + windowSize;
                        // 注册定时器
                        ctx.timerService().registerEventTimeTimer(windowEnd-1);
                        // 更新状态，如果之前就已经有了该窗口，直接累加，如果之前没有，就新增一个
                        if (mapState.contains(windowStart)){
                            mapState.put(windowStart, mapState.get(windowStart)+1L);
                        }else {
                            mapState.put(windowStart, 1L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        long windowEnd = timestamp + 1;
                        long windowStart = windowEnd - windowSize;
                        Long pv = mapState.get(windowStart);
                        out.collect("url: "+ctx.getCurrentKey()+"" +"\n"+
                                "在窗口"+new Timestamp(windowStart)+"-----"+new Timestamp(windowEnd)+"的访问量为："+pv);
                    }
                }).print();
        env.execute();
    }
}
