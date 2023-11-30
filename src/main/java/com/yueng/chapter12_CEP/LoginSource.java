package com.yueng.chapter12_CEP;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author RizzoYueng
 * @create 2023-10-14-15:35
 */
public class LoginSource implements SourceFunction<LoginEvent> {
    private Boolean running = true;
    @Override
    public void run(SourceContext<LoginEvent> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Marry", "Catalina", "Bob", "Josh"};
        String[] ips = {"192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4", "192.168.1.5"};
        String[] status = {"fail", "success"};
        Long[] intervals = {1000L, 1500L, 800L, 2000L};
        while (running){
            String userId = users[random.nextInt(users.length)];
            String ip = ips[random.nextInt(ips.length)];
            String state = status[random.nextInt(status.length)];
            long timestamp = System.currentTimeMillis();
            ctx.collect(new LoginEvent(userId,ip, state, timestamp));
            Thread.sleep(intervals[random.nextInt(intervals.length)]);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
