package com.yueng.chapter11_tableAPI;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-10-07-16:09
 */
public class TableEnvCreate {
    public static void main(String[] args) {
        // 2 基于流式处理环境构建表处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        // 1 定义环境配置来创建表执行环境，基于Blink的计划器
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // 基于老版本的planner
        EnvironmentSettings settings1 = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        TableEnvironment tableEnv2 = TableEnvironment.create(settings1);
        // 基于批处理的表执行环境
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);
        // 基于blink版本批处理
        EnvironmentSettings settings2 = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        TableEnvironment batchTableEnvironment1 = TableEnvironment.create(settings2);
    }
}
