package com.yueng.chapter9_operatorState;


import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author RizzoYueng
 * @create 2023-10-05-13:55
 */
public class BroadcastStateUserBehavior {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Catalina", "login"),
                new Action("Catalina", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "order")
        );
        // 需要匹配的模式，应该被广播的数据流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order")
        );
        // 定义广播状态描述器，创建广播流，用于将Pattern模式广播到所有的子任务
        MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(descriptor);
        // 连接两条流进行处理
        SingleOutputStreamOperator<Tuple2<String, Pattern>> matches = actionStream.keyBy(data -> data.user)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>() {
                    private ValueState<String> lastActionState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastActionState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-action-state", Types.STRING));
                    }

                    @Override
                    public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
                        String lastAction = lastActionState.value();
                        String currentAction = value.action;
                        // 从广播状态中获取匹配的规则
                        ReadOnlyBroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<Void, Pattern>("pattern", Types.VOID, Types.POJO(Pattern.class)));
                        Pattern pattern = patternState.get(null);
                        if (pattern != null && lastAction != null){
                            if (pattern.action1.equals(lastAction) && pattern.action2.equals(currentAction)){
                                out.collect(Tuple2.of(value+"配上了", pattern));
                            }
                        }
                        lastActionState.update(value.action);
                    }

                    @Override
                    public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
                        // 从上下文中获取广播状态并用当前数据更新状态
                        BroadcastState<Void, Pattern> pattern = ctx.getBroadcastState(new MapStateDescriptor<Void, Pattern>("pattern", Types.VOID, Types.POJO(Pattern.class)));
                        pattern.put(null, value);
                    }
                });
        matches.print();
        env.execute();
    }
    // 定义用户行为事件和模式的POJO类
    public static class Action{
        public String user;
        public String action;

        public Action() {
        }

        public Action(String user, String action) {
            this.user = user;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "user='" + user + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern{
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}
