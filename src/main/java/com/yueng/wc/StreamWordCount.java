package com.yueng.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author RizzoYueng
 * @create 2023-09-24-17:41
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
//        // 从程序参数中提取主机名和端口号
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String hostname = parameterTool.get("host");
//        Integer port = parameterTool.getInt("port");
        //2. 获取端口的流式数据
        DataStreamSource<String> lineDataStream = env.socketTextStream("hadoop102", 7777);
        //3. 监听端口
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).setParallelism(2).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedTuple = wordAndOneTuple.keyBy(data -> data.f0);
        //5. 统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedTuple.sum(1).setParallelism(2);
        // 输出
        sum.print();
        env.execute();
    }
}
