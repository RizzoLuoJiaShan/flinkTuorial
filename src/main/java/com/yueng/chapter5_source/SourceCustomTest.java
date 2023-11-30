package com.yueng.chapter5_source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @author RizzoYueng
 * @create 2023-09-26-15:27
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.addSource(new ParallelCustomSource()).setParallelism(2);
        streamSource.print();
        env.execute();
    }

    public static class ParallelCustomSource implements ParallelSourceFunction<Event> {
        private Boolean running = true;
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random();
            String[] users = {"catalina", "mary", "josh", "curry"};
            String[] urls = {"home", "cart", "detail", "payment"};
            while (running){
                String user = users[random.nextInt(users.length)];
                String url = urls[random.nextInt(urls.length)];
                ctx.collect(new Event(user, url, System.currentTimeMillis()));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
