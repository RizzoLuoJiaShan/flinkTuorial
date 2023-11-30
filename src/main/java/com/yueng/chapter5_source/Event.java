package com.yueng.chapter5_source;

import java.sql.Timestamp;

/**
 * @author RizzoYueng
 * @create 2023-09-26-10:36
 */
public class Event {
    public String username;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String username, String url, Long timestamp) {
        this.username = username;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "username='" + username + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
