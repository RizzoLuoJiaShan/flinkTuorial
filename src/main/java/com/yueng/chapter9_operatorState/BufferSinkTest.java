package com.yueng.chapter9_operatorState;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author RizzoYueng
 * @create 2023-10-05-13:16
 */
public class BufferSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000L); // 检查点配置
//        env.setStateBackend(new EmbeddedRocksDBStateBackend()) 配置状态后端的类型

        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        SingleOutputStreamOperator<String> mapped = source.map(data -> data.toString());
        mapped.print();
        mapped.addSink(new BufferSink(10));
        env.execute();
    }
    public static class BufferSink implements SinkFunction<String>, CheckpointedFunction {
        private Integer buffer;

        public BufferSink(Integer buffer) {
            this.buffer = buffer;
            this.bufferedElements = new ArrayList<>();
        }

        // 将一批次元素保存在列表中，再将列表中的元素写入到算子状态，算子状态通过checkPoint持久化
        private List<String> bufferedElements;
        private ListState<String> checkPointedState;

        @Override
        public void invoke(String value, Context context) throws Exception {
            bufferedElements.add(value);
            // 判断如果达到阈值就批量写入
            if (bufferedElements.size() == buffer){
                // 打印到控制台模拟写入外部系统
                for (String element : bufferedElements) {
                    System.out.println(element);
                }
                System.out.println("--------------当前输出完毕-------------");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 在同步列表时，先将之前的同步内容清空
            checkPointedState.clear();
            // 对状态进行持久化，复制缓存的列表到算子状态中
            checkPointedState.addAll(bufferedElements);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 第一次运行的定义状态
            ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("operator-list-state", Types.STRING);
            checkPointedState = context.getOperatorStateStore().getListState(listStateDescriptor);
            // 故障恢复，将状态中的元素复制到输出的本地列表中
            if (context.isRestored()){
                for (String str : checkPointedState.get()) {
                    bufferedElements.add(str);
                }
            }
        }
    }
}

