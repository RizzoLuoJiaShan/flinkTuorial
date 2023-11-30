package com.yueng.chapter5_sink;

import com.yueng.chapter5_source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author RizzoYueng
 * @create 2023-09-29-10:46
 */
public class TransformSinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从kafka读取数据
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));
        // 转换算子对数据进行转换处理
//        SingleOutputStreamOperator<Event> flattedMap = kafkaSource.flatMap((String data, Collector<Event> out) -> {
//            String[] columns = data.split(",");
//            Event event = new Event(columns[0], columns[1], Long.parseLong(columns[2]));
//            out.collect(event);
//        });
        // 这里不能使用lambda表达式是因为out中有泛型，会泛型擦除，但是returns无法声明Event的这个类型，所以选择使用匿名函数
        SingleOutputStreamOperator<Event> flattedMap = kafkaSource.flatMap(new FlatMapFunction<String, Event>() {
            @Override
            public void flatMap(String value, Collector<Event> out) throws Exception {
                String[] columns = value.split(",");
                Event event = new Event(columns[0].trim(), columns[1].trim(), Long.parseLong(columns[2].trim()));
                out.collect(event);
            }
        });
        // 将数据在一系列处理转换之后，再转换为String类型输出到kafka
        SingleOutputStreamOperator<String> outputStream = flattedMap.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.toString();
            }
        });
        outputStream.addSink(new FlinkKafkaProducer<String>("hadoop102:9092", "events", new SimpleStringSchema()));
        env.execute();
    }
}
