package com.yueng.chapter12_CEP;

import java.sql.Timestamp;

/**
 * @author RizzoYueng
 * @create 2023-10-14-20:56
 */
public class OrderEvent {
    public String userId;
    public String orderId;
    public String status;
    public Long timestamp;

    public OrderEvent() {
    }

    public OrderEvent(String userId, String orderId, String status, Long timestamp) {
        this.userId = userId;
        this.orderId = orderId;
        this.status = status;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", status='" + status + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
