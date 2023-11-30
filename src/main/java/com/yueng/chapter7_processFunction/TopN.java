package com.yueng.chapter7_processFunction;

import com.yueng.chapter5_source.ClickSource;
import com.yueng.chapter5_source.Event;
import com.yueng.chapter6_window.UrlCountView;
import com.yueng.chapter6_window.WindowURLCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author RizzoYueng
 * @create 2023-10-01-18:41
 */
public class TopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> source = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 每个窗口每个URL的统计
        SingleOutputStreamOperator<UrlCountView> urlCountStream = source.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new WindowURLCount.URLAgg(), new WindowURLCount.UrlPrintProcess());
        urlCountStream.print();
        /**对同一窗口统计出的访问量进行收集和排序
         * 因为此时的窗口计算已经结束了，窗口也已经关闭了，所以要想收集到当初窗口的所有URL的统计信息，就只能按照窗口的结束时间分组，把同一窗口内的URL统计数据都搜集在一起统一排序
         * 在按照窗口的结束时间分组之后，需要判断的是什么时候保证窗口内的所有URL的统计结果到齐了，虽然表面上看，统计结果是同时产生的，但是对于flink而言，数据仍然是stream的形式传输的
         * 因此需要设定一个定时器，在保证所有的URL统计数据都收集完毕后才排序
         * 因此使用KeyedProcessFunction中注册定时器，定时器的时长为：比窗口的结束时间晚1即可，只要水位线到达窗口结束时间后1个单位，表示窗口已经处理完毕，所有的数据都收集到了
         */
        urlCountStream.keyBy(data -> data.windowEnd).process(new TopNProcessResult(2)).print();
        env.execute();
    }

    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlCountView, String>{
        // 该属性用来传入最终输出的结果需要top几
        private Integer n;

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        // 该属性用于存储阶段内所有的url统计信息，是一个列表状态
        private ListState<UrlCountView> urlCountViewListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // urlCountViewListState不能直接new出来，而是应该根据执行的状态动态的根据key去生成每个key自己的状态列表
            urlCountViewListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlCountView>("url-count-view-list", Types.POJO(UrlCountView.class)));
        }

        @Override
        public void processElement(UrlCountView value, KeyedProcessFunction<Long, UrlCountView, String>.Context ctx, Collector<String> out) throws Exception {
            urlCountViewListState.add(value);
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey()+1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlCountView, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将状态列表中的数据放入ArrayList，然后清空状态列表
            ArrayList<UrlCountView> urlCountViews = new ArrayList<>();
            for (UrlCountView urlCountView : urlCountViewListState.get()) {
                urlCountViews.add(urlCountView);
            }
            // 清空状态列表
            urlCountViewListState.clear();
            // 对ArrayList中的元素排序
            urlCountViews.sort(new Comparator<UrlCountView>() {
                @Override
                public int compare(UrlCountView o1, UrlCountView o2) {
                    return o2.count - o1.count;
                }
            });
            // 取出前两位
            StringBuilder result = new StringBuilder();
            result.append("----------------------------窗口"+new Timestamp(ctx.getCurrentKey()-10000)+"~~"+new Timestamp(ctx.getCurrentKey())+"-----------------------------\n");
            for (Integer i = 0; i < n; i++) {
                String url = urlCountViews.get(i).url;
                Integer count = urlCountViews.get(i).count;
                result.append("No "+(i+1)+"访问量的URL是："+url+", 总访问量为："+count+"\n");
            }
            out.collect(result.toString());
        }
    }
}
