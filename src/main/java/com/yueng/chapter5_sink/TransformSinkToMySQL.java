package com.yueng.chapter5_sink;

import com.yueng.chapter5_source.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-09-29-12:22
 */
public class TransformSinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("Josh", "cart", 2000L),
                new Event("Silvia", "home", 4500L),
                new Event("Mark", "payment", 4000L),
                new Event("Josh", "order", 3000L),
                new Event("Josh", "payment", 2500L),
                new Event("Josh", "home", 3600L)
        );
        source.addSink(JdbcSink.sink(
                "insert into clicks(user, url) values(?,?)",
                ((statement, event) -> {
                    statement.setString(1, event.username);
                    statement.setString(2, event.url);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("abc123")
                        .build()
        ));
        env.execute();
    }
}
