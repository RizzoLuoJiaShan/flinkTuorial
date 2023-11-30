package com.yueng.chapter5_source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author RizzoYueng
 * @create 2023-09-26-15:28
 */
public class ClickSource implements SourceFunction<Event> {
    // 声明一个标志位
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 定义一个随机数生成器生成随机数据
        Random random = new Random();
        String[] users = {"Marry", "Catalina", "Bob", "Josh"};
        String[] urls = {"home?id=1", "cart?id=2", "detail?id=3", "order?id=4", "payment?id=5"};
        Long[] intervals = {1000L, 1500L, 800L, 2000L};
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = System.currentTimeMillis();
            ctx.collect(new Event(user, url, timestamp));
            Thread.sleep(intervals[random.nextInt(intervals.length)]);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
