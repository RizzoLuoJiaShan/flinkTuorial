package com.yueng.chapter9_keyedState;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-10-04-13:56
 */
public class ValueStateApply {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 实现的功能是：对所有的数据做一个统计PV，但是在特定的时间才输出，因为窗口只能输出当前窗口内的统计值，现在想要计算的是所有数据的统计值
        source.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    private ValueState<Integer> valueState;
                    // 用于保存定时器状态
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("PV-state", Types.INT));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-status", Types.LONG));
                    }

                    @Override
                    public void processElement(Event value, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        int currentPVCount = valueState.value() == null ? 0 : valueState.value();
                        valueState.update(currentPVCount+1);
                        // 如果当前还没有定时器的话就注册，当前没有定时器的话才注册，如果当时已经有了定时器，就不需要注册新的，保证输出的时间间隔是3S
                        if (timerTs.value() == null){
                            ctx.timerService().registerEventTimeTimer(ctx.timestamp()+3000L);
                            timerTs.update(ctx.timestamp());
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("当前的总PV为："+valueState.value());
                        // 将保存定时器的状态清空
                        timerTs.clear();
                    }
                }).print();

        env.execute();
    }
}
