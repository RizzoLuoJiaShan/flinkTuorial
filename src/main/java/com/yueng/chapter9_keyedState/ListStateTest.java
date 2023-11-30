package com.yueng.chapter9_keyedState;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-10-04-13:31
 */
public class ListStateTest {
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
                        })
                );
        source.keyBy(data -> data.username).flatMap(new RichFlatMapFunction<Event, String>() {
            private ListState<Event> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<>("event-state-list", Types.POJO(Event.class)));
            }

            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                listState.add(value);
                System.out.println("当前的listState包含的Event对象-------------");
                for (Event event : listState.get()) {
                    System.out.println(event.toString());
                }
                System.out.println("-------------------------------------------------------");
            }
        }).print();
        env.execute();
    }
}
