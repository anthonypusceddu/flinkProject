package Query1;

import com.google.common.collect.Lists;
import model.Config;
import model.Post;
import model.PostSchema;
import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {


    public static FlinkKafkaConsumer<Post> createStringConsumerForTopic(
            String topic, String kafkaAddress, String kafkaGroup ) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<Post> consumer;
        consumer = new FlinkKafkaConsumer<>(topic, new PostSchema(),props);
        //consumer.setStartFromEarliest();
        consumer.setStartFromLatest();

        return consumer;
    }

    private static FlinkKafkaProducer<String> createStringProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer<>(kafkaAddress, topic, new SimpleStringSchema());
    }


    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        //create environment
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // set EVENT_TIME
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //Create kafka consumer
        FlinkKafkaConsumer<Post> flinkKafkaConsumer = createStringConsumerForTopic(
                Config.TOPIC, Config.kafkaBrokerList, Config.consumerGroup);

        //Take timestamp from kafka consumer tuple
        flinkKafkaConsumer.assignTimestampsAndWatermarks(new PostTimestampAssigner());

        // create kafka producer
        FlinkKafkaProducer<String> flinkKafkaProducer =
                createStringProducer(Config.OutTOPIC, Config.kafkaBrokerList);

        //stream data from kafka consumer
        DataStream<Post> stringInputStream = environment
                .addSource(flinkKafkaConsumer);

        DataStream<Tuple3<Long,String, Integer>> temp = stringInputStream
                .map(new MapFunction<Post, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Post post) throws Exception {
                        return new Tuple2<>(post.getArticleId(),1);
                    }
                })
                .keyBy(t -> t.f0)
                .timeWindow(Time.days(1))
                //.window(SlidingEventTimeWindows.of(Time.hours(1),Time.seconds(5)) )
                .process(new CountWindowsArticle());
        //temp.print();



        DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> print = temp
                .keyBy(r->r.f1)
                .timeWindowAll(Time.days(1))
                .apply(new AllWindowFunction<Tuple3<Long, String, Integer>, Tuple2<Long,List<Tuple2<String, Integer>>>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple3<Long, String, Integer>> iterable, Collector<Tuple2<Long,List<Tuple2<String, Integer>>>> collector) throws Exception {
                        //System.out.println("WindowsAll "+ "size: "+ IterableUtils.size(iterable));
                        List<Tuple2<String, Integer>> list = new ArrayList<>();
                        long min = iterable.iterator().next().f0;
                        for( Tuple3<Long, String, Integer> t : iterable){
                            if (t.f0<min){
                                min = t.f0;
                            }
                            list.add(new Tuple2<>(t.f1,t.f2));
                        }
                        list = list
                                .stream()
                                .sorted((o1, o2) -> Integer.compare(o2.f1,o1.f1) )
                                .limit(3).collect(Collectors.toList());
                        collector.collect(new Tuple2<>(min,list));
                    }
                });
        //.addSink(flinkKafkaProducer);
        print.print();
        environment.setParallelism(1);
        environment.execute("prova");
    }





    public static class CountWindowsArticle extends ProcessWindowFunction<
            Tuple2<String,Integer>, Tuple3<Long, String, Integer>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String,Integer>> articleList, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
          //  int count= 0;
           // System.out.println("Windows "+ "key:" + key + "size: "+ IterableUtils.size(articleList));
          /*  for (Tuple2<String,Integer> a : articleList) {
                count += 1;
            }*/
            //System.out.println(context.window());
            out.collect(new Tuple3<>(context.window().getStart(), key, IterableUtils.size(articleList)));
        }
    }
    
}

