package com.yueng.chapter9_keyedState;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author RizzoYueng
 * @create 2023-10-04-15:25
 */
public class AggregatingStateApply {
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
        source.keyBy(data -> data.username)
                .flatMap(new UserAggTimestamp(5L))
                .print();

        env.execute();
    }
    public static class UserAggTimestamp extends RichFlatMapFunction<Event, String> {
        // 该属性用于判断是否应该输出
        private Long printThreshold;
        private AggregatingState<Event, String> aggregatingState;
        private ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, String>(
                    "agg-timestamp",
                    new AggregateFunction<Event, Tuple2<Long, Long>, String>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0+value.timestamp, accumulator.f1+1);
                        }

                        @Override
                        public String getResult(Tuple2<Long, Long> accumulator) {
                            return String.valueOf(accumulator.f0 / accumulator.f1);
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.POJO(Event.class), Types.LONG)
            ));
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("count-state", Types.LONG));
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("State-value", Types.POJO(Event.class));
            valueStateDescriptor.enableTimeToLive(ttlConfig);

        }

        public UserAggTimestamp(Long printThreshold) {
            this.printThreshold = printThreshold;
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            long currentCount = valueState.value() == null ? 0 : valueState.value();
            if (currentCount >= printThreshold){
                out.collect(value.username+"过去"+printThreshold+"次访问的平均时间戳为："+aggregatingState.get());
                aggregatingState.clear();
                valueState.clear();
            }else {
                valueState.update(currentCount+1);
                aggregatingState.add(value);
            }
        }
    }
}
