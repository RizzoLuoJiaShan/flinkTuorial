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
 * @create 2023-10-07-15:36
 */
public class TableAPITest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventSource = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 创建一个表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将dataStream转换为table
        Table eventTable = tableEnv.fromDataStream(eventSource);
        // 写SQL将dataStream转换为Table
        Table resultTable = tableEnv.sqlQuery("select username, url, `timestamp` from " + eventTable);
        //tableEnv.toDataStream(resultTable).print();
        // 基于table之间转换
        Table resultTable2 = eventTable.select($("username"), $("url"))
                .where($("username").isEqual("Catalina"));
        // 转换成流输出
        tableEnv.toDataStream(resultTable2).print();
        env.execute();
    }
}
