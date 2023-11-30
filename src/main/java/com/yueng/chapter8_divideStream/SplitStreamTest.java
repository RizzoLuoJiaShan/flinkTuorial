package com.yueng.chapter8_divideStream;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-10-02-14:01
 */
public class SplitStreamTest {
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
        // 基于source做分流
        // 定义输出标签，因为泛型擦除，所以以内部类的形式再传入泛型
        OutputTag<Tuple3<String, String, Long>> catalinaTag = new OutputTag<Tuple3<String, String, Long>>("Catalina"){};
        OutputTag<Tuple3<String, String, Long>>  bobTag= new OutputTag<Tuple3<String, String, Long>>("Bob"){};

        SingleOutputStreamOperator<Event> processStream = source.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.username.equals("Catalina")) {
                    ctx.output(catalinaTag, Tuple3.of(value.username, value.url, value.timestamp));
                } else if (value.username.equals("Bob")) {
                    ctx.output(bobTag, Tuple3.of(value.username, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }
        });
        processStream.print("else");
        processStream.getSideOutput(catalinaTag).print("catalina");
        processStream.getSideOutput(bobTag).print("Bob");
        env.execute();
    }
}
