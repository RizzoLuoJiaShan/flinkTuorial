package com.yueng.chapter6_watermark;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-09-29-15:54
 */
public class WatermarkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100L);
        SingleOutputStreamOperator<Event> watermarkedSource = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("josh", "cart", 2000L),
                new Event("bob", "detail", 3000L),
                new Event("alice", "order", 4000L),
                new Event("fish", "payment", 5000L)
                // 有序流的watermark策略生成
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));
        // 乱序流的watermark的策略生成，在实际场景下，都是乱序流，因为存在消息队列的分布式处理，框架计算算子的并行处理都会造成数据乱序
        SingleOutputStreamOperator<Event> watermarkedSource1 = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("josh", "cart", 2000L),
                new Event("bob", "detail", 3000L),
                new Event("alice", "order", 4000L),
                new Event("fish", "payment", 5000L)
                // 有序流的watermark策略生成
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

    }
}
