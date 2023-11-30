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
import org.apache.flink.table.functions.AggregateFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author RizzoYueng
 * @create 2023-10-12-19:36
 */
public class UDFAggregateFunction {
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
        Table sourceTable = tableEnv.fromDataStream(source, $("username"), $("url"), $("timestamp").as("ts"), $("et").rowtime());
        tableEnv.createTemporaryView("clickTable", sourceTable);
        tableEnv.createTemporarySystemFunction("MyAggregateFunction", MyAggregateFunction.class);
        Table result = tableEnv.sqlQuery("select username, MyAggregateFunction(ts, 1) from clickTable group by username");
        tableEnv.toChangelogStream(result).print();
        env.execute();
    }
    public static class WeightedAvgAcc{
        public long sum = 0;
        public int count = 0;
    }
    // 创建一个自定义的聚合函数 UDAF，计算加权平均数
    public static class MyAggregateFunction extends AggregateFunction<Long, WeightedAvgAcc>{
        @Override
        public Long getValue(WeightedAvgAcc accumulator) {
            if(accumulator.count == 0){
                return null;
            }else{
                return accumulator.sum / accumulator.count;
            }
        }

        @Override
        public WeightedAvgAcc createAccumulator() {
            return new WeightedAvgAcc();
        }
        // 累加计算的方法
        public void accumulate(WeightedAvgAcc accumulator, Long myValue, Integer myWeight){
            accumulator.sum+=myValue*myWeight;
            accumulator.count += myWeight;
        }
    }
}
