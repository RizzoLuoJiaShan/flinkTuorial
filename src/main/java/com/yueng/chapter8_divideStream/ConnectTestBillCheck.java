package com.yueng.chapter8_divideStream;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author RizzoYueng
 * @create 2023-10-02-15:43
 */
public class ConnectTestBillCheck {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200);
        // 先定义一个来自APP的数据源
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order1", "app", 1000L),
                Tuple3.of("order3", "app", 4000L),
                Tuple3.of("order4", "app", 3000L),
                Tuple3.of("order2", "app", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );
        // 再定义一个来自第三方支付平台的支付记录数据源
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartyStream = env.fromElements(
                Tuple4.of("order1", "third", "success", 3000L),
                Tuple4.of("order3", "third", "success", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                        return element.f3;
                    }
                })
        );
        // 使用connect算子将两个流连接起来，可以先keyBy再连接，也可以直接连接后再keyBy
        ConnectedStreams<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>> connected = appStream.connect(thirdPartyStream)
                .keyBy(data -> data.f0, data -> data.f0);

        connected.process(new BillCheckCoProcessFunction()).print();

        env.execute();
    }

    public static class BillCheckCoProcessFunction extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>{

        // 定义两个状态变量，为了保存已经到达的order信息，从而对照是不是能匹配
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event-list", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdEventState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String, String,  Long>>("third-event-list", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            if (thirdEventState.value() != null){
                out.collect("对账成功"+value+"third先到"+thirdEventState.value());
                thirdEventState.clear();
            }else{
                appEventState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f2+5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            if(appEventState.value() != null){
                out.collect("对账成功"+appEventState.value()+"app先到"+value);
                appEventState.clear();
            }else{
                thirdEventState.update(value);
                // 一般来说，是app的数据先到，third的数据后到，但是有可能app的数据乱序了，所以如果到了third的水位线，app的数据一定是有的
                ctx.timerService().registerEventTimeTimer(value.f3);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 判断状态，如果某个状态不为空，说明另一条流的数据没来
            if(appEventState.value() != null){
                out.collect("对账失败："+appEventState.value()+"，第三方支付平台信息未到");
            }
            if (thirdEventState.value() != null){
                out.collect("对账失败："+thirdEventState.value()+"，app支付信息未到");
            }
            appEventState.clear();
            thirdEventState.clear();
        }
    }
}
