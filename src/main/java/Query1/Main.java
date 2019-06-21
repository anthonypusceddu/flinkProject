package Query1;


import model.Config;
import model.Post;
import model.PostSchema;
import org.apache.commons.collections4.IterableUtils;


import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;


import java.util.*;
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

        DataStream<Tuple3<Long,String, Integer>> hour = stringInputStream
                .map(new MapFunction<Post, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Post post) throws Exception {
                        return new Tuple2<>(post.getArticleId(),1);
                    }
                })
                .keyBy(t -> t.f0)
                .timeWindow(Time.hours(1))
                //.window(SlidingEventTimeWindows.of(Time.hours(1),Time.seconds(10)) )
                .process(new CountWindowsArticle());

        //hour.print();

        /*DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> hourStat = hour
                .timeWindowAll(Time.hours(1))
                .apply(new TopN())
                .setParallelism(1);
        hourStat.print();*/


        /*DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> DayStat = hour
                .timeWindowAll(Time.hours(24),Time.hours(1))
                .apply(new TopN_sliding())
                .setParallelism(1);
        DayStat.print();*/


        DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> WeekStat = hour
                .timeWindowAll(Time.days(7),Time.hours(24))
                .apply(new TopN_sliding())
                .setParallelism(1);
        WeekStat.print();


        //.addSink(flinkKafkaProducer);*/
        //environment.setParallelism(1);
        environment.execute("prova");
    }

    public static class TopN_sliding implements AllWindowFunction<Tuple3<Long, String, Integer>, Tuple2<Long,List<Tuple2<String, Integer>>>, TimeWindow> {
        @Override
        public void apply(TimeWindow timeWindow, Iterable<Tuple3<Long, String, Integer>> iterable, Collector<Tuple2<Long,List<Tuple2<String, Integer>>>> collector) throws Exception {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            HashMap<String,Integer> map = new HashMap<>();
            for( Tuple3<Long, String, Integer> t : iterable){
                String key = t.f1;
                int value = t.f2;
                if (map.containsKey(key)){
                    map.put(key,map.get(key)+value);
                }else {
                    map.put(key, value);
                }
            }
            for ( String k : map.keySet()){
                Tuple2<String,Integer> tuple= new Tuple2<>(k,map.get(k));
                list.add(tuple);
            }
            list = list
                    .stream()
                    .sorted((o1, o2) -> Integer.compare(o2.f1,o1.f1) )
                    .limit(3).collect(Collectors.toList());
            collector.collect(new Tuple2<>(timeWindow.getEnd(),list));
        }
    }

    public static class TopN implements AllWindowFunction<Tuple3<Long, String, Integer>, Tuple2<Long,List<Tuple2<String, Integer>>>, TimeWindow> {
        @Override
        public void apply(TimeWindow timeWindow, Iterable<Tuple3<Long, String, Integer>> iterable, Collector<Tuple2<Long,List<Tuple2<String, Integer>>>> collector) throws Exception {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for( Tuple3<Long, String, Integer> t : iterable){
                list.add(new Tuple2<>(t.f1,t.f2));
            }
            list = list
                    .stream()
                    .sorted((o1, o2) -> Integer.compare(o2.f1,o1.f1) )
                    .limit(3).collect(Collectors.toList());
            collector.collect(new Tuple2<>(timeWindow.getEnd(),list));
        }
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

