package com.yueng.chapter11_tableAPI;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-10-07-16:39
 */
public class QueryApi {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // 注册输入表
        tableEnv.executeSql("create table eventTable (user_name STRING, url STRING, `timestamp` STRING) with ('connector' = 'filesystem', 'path'='input/clicks.txt','format'='csv')");
        // 查询
        Table catalinaClickTable = tableEnv.sqlQuery("select user_name, url from eventTable where user_name = 'Catalina'");
        // 注册输出表
        tableEnv.executeSql("create table outTable (user_name STRING, url STRING) with ('connector' = 'filesystem', 'path'='output', 'format'='csv')");
        // 查询结果Table对象写入输出表中
        catalinaClickTable.executeInsert("outTable");
        // 创建一张用于控制台打印输出的表
        tableEnv.executeSql("create table outTablePrint (url STRING, times BIGINT) with ('connector' = 'print')");
        Table result = tableEnv.sqlQuery("select user_name, count(*) from eventTable group by user_name");
        result.executeInsert("outTablePrint");
    }
}
