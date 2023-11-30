package com.yueng.chapter12_CEP;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @author RizzoYueng
 * @create 2023-10-14-20:26
 */
public class LoginFailDetectProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<LoginEvent> source = env.addSource(new LoginSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 定义复杂事件模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.status.equals("fail");
                    }
                }).times(3).consecutive(); // 直接重复三次，且之间是严格近邻
        // 将Pattern应用于流式数据
        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(data -> data.userId), pattern);
        // 提取复杂事件，报警信息
        SingleOutputStreamOperator<String> processed = patternStream.process(new PatternProcessFunction<LoginEvent, String>() {
            @Override
            public void processMatch(Map<String, List<LoginEvent>> match, Context ctx, Collector<String> out) throws Exception {
                // 提取三次登录失败事件
                LoginEvent firstFail = match.get("fail").get(0);
                LoginEvent secondFail = match.get("fail").get(1);
                LoginEvent thirdFail = match.get("fail").get(2);
                out.collect(firstFail.userId + "连续三次登录失败！"
                        + "第1次登录时间为：" + new Timestamp(firstFail.timestamp)
                        + "第2次登录时间为：" + new Timestamp(secondFail.timestamp)
                        + "第3次登录时间为：" + new Timestamp(thirdFail.timestamp));
            }
        });
        processed.print();
        env.execute();
    }
}
