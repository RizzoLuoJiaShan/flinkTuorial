package com.yueng.chapter11_tableAPI;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-10-07-16:24
 */
public class TableCreate {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // 创建一张用于输入的表
        String createDDL = "create table clickTable (user_name STRING, url STRING, ts BIGINT ) with ('connector' = 'filesystem', 'path'='input/clicks.txt', 'format'='csv')";
        tableEnv.executeSql(createDDL);
        // 创建一张用于输出的表
        String createOutDDL = "create table outTable (user_name STRING, url STRING) with ('connector' = 'filesystem', 'path'='output', 'format'='csv')";
        tableEnv.executeSql(createOutDDL);
    }
}
