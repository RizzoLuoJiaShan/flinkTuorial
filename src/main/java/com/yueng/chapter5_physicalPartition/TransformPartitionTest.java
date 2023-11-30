package com.yueng.chapter5_physicalPartition;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author RizzoYueng
 * @create 2023-09-28-16:13
 */
public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> source = env.fromElements(
                new Event("catalina", "home", 1000L),
                new Event("Josh", "cart", 2000L),
                new Event("Silvia", "home", 4500L),
                new Event("Mark", "payment", 4000L),
                new Event("Josh", "order", 3000L),
                new Event("Josh", "payment", 2500L),
                new Event("Josh", "home", 3600L)
        );
        // 1 分区方式——shuffle，随机分区
//        source.shuffle().print().setParallelism(4);
        // 2 分区方式——轮询分区 round robin，默认的方式就是轮询
//        source.rebalance().print().setParallelism(4);
        // 3 分区方式——重缩放分区 rescale
        /**
         * rebalance：每个发牌人都针对下游的所有人发牌。针对所有的分区数据重新平衡，如果TaskManager数量多，跨节点的网络传输会影响性能
         * rescale：分成小团体，每个发牌人只针对自己小团体发牌。只要上下游之间的并行度是整数倍，就可以只在自己的TaskManager中重新分配
         */
//        env.addSource(new RichParallelSourceFunction<Integer>() {
//            @Override
//            public void run(SourceContext<Integer> ctx) throws Exception {
//                for (int i = 1; i <= 8; i++) {
//                    // 将奇数偶数分别发送到1号和0号并行分区
//                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
//                        ctx.collect(i);
//                    }
//                }
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        }).setParallelism(2).rescale().print().setParallelism(4);

        // 4 广播，将数据发送到下游所有的slot中，会造成重复执行
        // 5 全局模式，将数据全部发送到同一个slot中，会造成某个节点的压力过大
        // 6 自定义模式
        env.fromElements(1,2,3,4,5,6,7,8).partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % 2;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return value;
            }
        }).print().setParallelism(4);
        env.execute();
    }
}
