package com.yueng.chapter11_tableAPI;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author RizzoYueng
 * @create 2023-10-12-15:07
 */
public class UDFTableFunction {
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
        // 注册表函数
        tableEnv.createTemporarySystemFunction("MyTableFunction", MyTableFunction.class);
        // 使用UDTF函数查询
        Table result = tableEnv.sqlQuery("select username, url, word, length " +
                "from clickTable, LATERAL TABLE(MyTableFunction(url)) AS T(word, length)");
        tableEnv.toDataStream(result).print();
        env.execute();
    }
    // 实现自定义表函数
    public static class MyTableFunction extends TableFunction<Tuple2<String, Integer>>{
        public void eval(String str){
            String[] fields = str.split("\\?");
            for (String field:fields){
                collect(Tuple2.of(field, field.length()));
            }
        }
    }
}
