package com.yueng.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author RizzoYueng
 * @create 2023-09-24-16:44
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 创建一个执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/wc.txt");
        // 将每行数据分词，转换为二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将一行文本进行拆分
            String[] words = line.split(" ");
            // 将每个单词转换为二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 按照word分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        // 统计，分组内聚合
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
        // 结果输出
        sum.print();
    }
}
