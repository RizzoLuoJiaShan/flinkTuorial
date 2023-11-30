package com.yueng.chapter5_transform;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-09-28-15:18
 */
public class TransformRichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("Josh", "cart", 3000L),
                new Event("Lisa", "detail", 4000L),
                new Event("Linda", "payment", 5000L)
        );
        source.map(new RichMapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.username;
            }
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("open生命周期被调用"+getRuntimeContext().getIndexOfThisSubtask()+"号任务启动");
            }
            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("close生命周期被调用"+getRuntimeContext().getIndexOfThisSubtask()+"号任务结束");
            }
        }).setParallelism(2).print();
        env.execute();
    }
}
