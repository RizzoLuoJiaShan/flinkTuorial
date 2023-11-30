package com.yueng.chapter11_tableAPI;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author RizzoYueng
 * @create 2023-10-10-16:13
 */
public class TableAggApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 先创建一个流，然后转为表
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 将流转换为Table对象，提取时间属性
        Table sourceTable = tableEnv.fromDataStream(source, $("username"), $("url"), $("timestamp").as("ts"), $("et").rowtime());
        // 将Table对象注册为可以表目录中的表
        tableEnv.createTemporaryView("clickTable", sourceTable);
        // 定义窗口和查询
        Table result = tableEnv.sqlQuery("select username, count(*) as cnt, TUMBLE_END(et, INTERVAL '10' SECOND) as endT " +
                " from clickTable " +
                " group by" +
                " username, TUMBLE(et, INTERVAL '10' SECOND)"
        );
        tableEnv.toChangelogStream(result).print();
        env.execute();
    }
}
