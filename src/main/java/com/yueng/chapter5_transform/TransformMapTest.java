package com.yueng.chapter5_transform;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-09-26-18:29
 */
public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("catalina", "./home", 1000L),
                new Event("Lisa", "./cart", 2000L),
                new Event("catalina", "./detail", 3000L),
                new Event("Josh", "./payment", 4000L)
                );
        // 进行转换计算提取user字段
        SingleOutputStreamOperator<String> mapStream1 = streamSource.map(new MyMapper());
        // 使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> mapStream2 = streamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.username;
            }
        });
        // 使用lambda表达式
        SingleOutputStreamOperator<String> mapStream3 = streamSource.map(data -> data.username);
        mapStream3.print();
        env.execute();
    }

    // 自定义MapFunction
    public static class MyMapper implements MapFunction<Event, String>{
        @Override
        public String map(Event value) throws Exception {
            return value.username;
        }
    }
}
