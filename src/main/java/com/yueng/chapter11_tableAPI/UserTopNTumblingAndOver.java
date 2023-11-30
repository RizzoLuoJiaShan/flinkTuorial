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
 * @create 2023-10-11-14:02
 */
public class UserTopNTumblingAndOver {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 流转换为Table对象
        Table sourceTable = tableEnv.fromDataStream(source, $("username"), $("url"), $("timestamp").as("ts"), $("et").rowtime());
        tableEnv.createTemporaryView("clickTable", sourceTable);
        // 先开一个窗口统计窗口内每个用户的访问量
        Table tumbleTable = tableEnv.sqlQuery("select window_start, window_end, username, count(1)as cnt " +
                "from table(TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)) " +
                "group by username, window_start, window_end");
        tableEnv.createTemporaryView("tumbleTable", tumbleTable);
        // 在开窗的基础上，针对每个窗口内的用户访问量排序
        Table windowTopN = tableEnv.sqlQuery("select username, cnt, row_num, window_end from " +
                "(select *, ROW_NUMBER() over(partition by window_start, window_end order by cnt)as row_num from tumbleTable)" +
                "where row_num <= 2");
        tableEnv.toChangelogStream(windowTopN).print();
        env.execute();
    }
}
