package com.yueng.chapter11_tableAPI;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author RizzoYueng
 * @create 2023-10-12-20:03
 */
public class UDFTableAggregateFunction {
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
        // 先定义一个窗口，统计窗口内各个URL出现的次数
        Table tumbleWindowTable = tableEnv.sqlQuery("select url, window_end, count(1) as cnt " +
                "from table(TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)) " +
                "group by url, window_start, window_end");
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);
        // 之后对窗口内统计的结果直接使用表聚合函数求TOP2
        Table result = tumbleWindowTable.groupBy("window_end")
                // 因为在定义表聚合函数的时候输入的参数是两个，所以这里也需要传入两个参数
                .flatAggregate(call("Top2", $("url"), $("cnt")).as("urlName", "value", "rank"))
                .select($("window_end"), $("urlName"), $("value"), $("rank"));
        tableEnv.toChangelogStream(result).print();
        env.execute();
    }

    // 定义一个累加器类型，包含当前最大和第二大数据
    public static class Top2Accumulate {
        public Tuple2<String, Long> max;
        public Tuple2<String, Long> secondMax;
    }

    public static class Top2 extends TableAggregateFunction<Tuple3<String, Long, Integer>, Top2Accumulate> {

        @Override
        public Top2Accumulate createAccumulator() {
            Top2Accumulate top2Accumulate = new Top2Accumulate();
            top2Accumulate.max = Tuple2.of(" ", Long.MIN_VALUE);
            top2Accumulate.secondMax = Tuple2.of(" ", Long.MIN_VALUE);
            return top2Accumulate;
        }

        /**
         * @param accumulator 累加器的上一个状态
         * @param urlName 输入的原始参数1——URL
         * @param value 输入的原始参数2——URL出现的次数
         */
        public void accumulate(Top2Accumulate accumulator, String urlName, Long value) {
            if (value > accumulator.max.f1) {
                accumulator.secondMax = accumulator.max;
                accumulator.max = Tuple2.of(urlName, value);
            } else if (value > accumulator.secondMax.f1) {
                accumulator.secondMax = Tuple2.of(urlName, value);
            } else if (value == accumulator.max.f1) {
                accumulator.secondMax = Tuple2.of(urlName, value);
            }
        }

        public void emitValue(Top2Accumulate accumulator, Collector<Tuple3<String, Long, Integer>> out) {
            if (accumulator.max.f1 != Long.MIN_VALUE) {
                out.collect(Tuple3.of(accumulator.max.f0, accumulator.max.f1, 1));
            }
            if (accumulator.secondMax.f1 != Long.MIN_VALUE) {
                out.collect(Tuple3.of(accumulator.secondMax.f0, accumulator.secondMax.f1, 2));
            }
        }
    }
}
