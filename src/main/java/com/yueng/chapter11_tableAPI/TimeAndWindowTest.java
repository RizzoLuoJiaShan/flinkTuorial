package com.yueng.chapter11_tableAPI;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author RizzoYueng
 * @create 2023-10-09-17:38
 */
public class TimeAndWindowTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 事件时间语义方式1：在创建表的DDL中直接定义时间属性
        String createDDL = "create table clickTable (" +
                "user_name STRING" +
                "url STRING" +
                "ts BIGINT" +
                // 这里要声明一个事件时间的assigner，TO_TIMESTAMP需要传入一个年月日时间，所以使用FROM_UNIXTIME函数，该函数参数必须是秒为单位
                "et AS TO_TIMESTAMP( FROM_UNIXTIME(ts/1000) )," +
                // 水位线生成器，指定水位线时间为基于事件时间eventTime延迟1秒
                "WATERMARK FOR et AS et - INTERVAL '1' SECOND" +
                ") with (" +
                "'connector' = 'filesystem'," +
                "'path' = 'input/clicks.txt'," +
                "'format' = 'csv'" +
                ")";
        // 事件时间语义方式2：在流转换成表的时候定义时间属性
        SingleOutputStreamOperator<Event> streamSource = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        Table clickTable = tableEnv.fromDataStream(streamSource, $("username"), $("url"), $("timestamp").as("ts"), $("et").rowtime());
        clickTable.printSchema();
        // 处理时间语义方式1：在创建表的时候定义
        String createDDL2 = "create table clickTable2 (user_name STRING, url STRING, timestamp BIGINT, pt AS PROCTIME()) " +
                "with ('connector'='filesystem', 'path' = 'input/clicks.txt', 'format'='csv')";
        // 处理时间语义方式2：在流转换成表的时候定义时间属性
        SingleOutputStreamOperator<Event> streamSource2 = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        Table clickTable2 = tableEnv.fromDataStream(streamSource2, $("user_name"), $("url"), $("pt"), $("ts").proctime());
    }
}
