package com.yueng.chapter12_CEP;

import java.sql.Timestamp;

/**
 * @author RizzoYueng
 * @create 2023-10-14-15:31
 */
public class LoginEvent {
    public String userId;
    public String ipAddress;
    public String status;
    public Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ipAddress, String status, Long timestamp) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.status = status;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", status='" + status + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
