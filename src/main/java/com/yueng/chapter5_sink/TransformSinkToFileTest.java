package com.yueng.chapter5_sink;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author RizzoYueng
 * @create 2023-09-28-17:30
 */
public class TransformSinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(200L);
        DataStreamSource<Event> source = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("Josh", "cart", 2000L),
                new Event("Silvia", "home", 4500L),
                new Event("Mark", "payment", 4000L),
                new Event("Josh", "order", 3000L),
                new Event("Josh", "payment", 2500L),
                new Event("Josh", "home", 3600L)
        );
        // forRowFormat需要在前面泛型传入处理的数据类型，参数中传入的是输出的路径和数据的编码格式
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024).withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)).build()
                ).build();
        source.map(data -> data.toString()).addSink(streamingFileSink);
        env.execute();
    }
}
