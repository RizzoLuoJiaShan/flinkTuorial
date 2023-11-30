package com.yueng.chapter12_CEP;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @author RizzoYueng
 * @create 2023-10-14-20:58
 */
public class OrderTimeoutDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<OrderEvent> source = env.fromElements(
                new OrderEvent("catalina", "A001", "order", 1000L),
                new OrderEvent("catalina", "A002", "order", 2000L),
                new OrderEvent("catalina", "A003", "order", 3000L),
                new OrderEvent("catalina", "A001", "payment", 4000L),
                new OrderEvent("catalina", "A002", "payment", 6000L),
                new OrderEvent("catalina", "A004", "order", 9000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("order")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.status.equals("order");
                    }
                })
                .followedBy("payment")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.status.equals("payment");
                    }
                }).within(Time.seconds(60L));
        PatternStream<OrderEvent> patternStream = CEP.pattern(source.keyBy(data -> data.orderId), pattern);
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};
        SingleOutputStreamOperator<String> result = patternStream.process(new MyPatternProcessFunction());
        result.print("正常支付的订单数据");
        result.getSideOutput(timeoutTag).print("超时未支付的订单数据");
        env.execute();
    }

    public static class MyPatternProcessFunction extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent> {

        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            OrderEvent order = match.get("order").get(0);
            OrderEvent payment = match.get("payment").get(0);
            out.collect(order.userId + "针对订单" + order.orderId
                    + "\n" + "的下单时间为：" + new Timestamp(order.timestamp)
                    + "\n" + "支付时间为：" + new Timestamp(payment.timestamp));
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            OrderEvent order = match.get("order").get(0);
            OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};
            ctx.output(timeoutTag, "用户"+order.userId+"的订单"+order.orderId+"超时未支付");
        }
    }
}
