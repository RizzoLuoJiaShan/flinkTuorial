package com.yueng.chapter6_window;

import java.sql.Timestamp;

/**
 * @author RizzoYueng
 * @create 2023-10-01-11:37
 */
public class UrlCountView {
    public String url;
    public Integer count;
    public Long windowStart;
    public Long windowEnd;

    public UrlCountView() {
    }

    public UrlCountView(String url, Integer count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlCountView{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
