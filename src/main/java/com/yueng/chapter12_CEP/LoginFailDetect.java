package com.yueng.chapter12_CEP;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.common.security.auth.Login;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @author RizzoYueng
 * @create 2023-10-14-15:31
 */
public class LoginFailDetect {
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
        // 定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first")  // 第一次登录失败
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.status.equals("fail");
                    }
                })
                .next("second") // 第二次登录失败
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.status.equals("fail");
                    }
                })
                .next("third") // 第三次登录失败
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.status.equals("fail");
                    }
                });
        // 将模式应用于数据流，检测复杂事件CE
        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(data -> data.userId), pattern); // 记得要对source进行user分组

        // 将检测到的CE提取出来，进行处理得到报警信息
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception { // 这里的key是简单事件的名称，每个标签内部可能有很多个值
                // 提取复杂事件中的三次登录失败事件
                List<LoginEvent> firstLoginFail = map.get("first");
                List<LoginEvent> secondLoginFail = map.get("second");
                List<LoginEvent> thirdLoginFail = map.get("third");
                return firstLoginFail.get(0).userId + "连续三次登录失败"
                        + "\n" + firstLoginFail
                        + "\n" + secondLoginFail
                        + "\n" + thirdLoginFail;
            }
        });
        source.print();
        result.print();
        env.execute();
    }
}
