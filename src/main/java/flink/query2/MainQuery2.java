package flink.query2;

import model.Post;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import flink.query2.operators.AggregateTimeSlot;
import flink.query2.operators.AggregateTimeSlotSliding;
import flink.query2.operators.CountTimeSlotComment;
import utils.Config;
import utils.FlinkUtils;
import utils.KafkaUtils;
import utils.PostTimestampAssigner;

import java.util.Map;

public class MainQuery2 {

    public static void main(String[] args) throws Exception {

        //create environment
        StreamExecutionEnvironment environment = FlinkUtils.setUpEnvironment(args);

        //Create kafka consumer
        FlinkKafkaConsumer<Post> flinkKafkaConsumer = KafkaUtils.createStringConsumerForTopic(
                Config.TOPIC, Config.kafkaBrokerList, Config.consumerGroup);

        //Take timestamp from kafka consumer tuple
        flinkKafkaConsumer.assignTimestampsAndWatermarks(new PostTimestampAssigner());

        //stream data from kafka consumer
        DataStream<Post> stringInputStream = environment
                .addSource(flinkKafkaConsumer);


        /*query starts here
          filter only direct comments
          map to (TimeSlot, occurencies set to 1)
          keyBy
          tumbling window of 24 hours / 7 days / 1 month
          custom function apply
         */
        DataStream<Tuple2<Integer, Integer>> day = stringInputStream
                .filter(new FilterFunction<Post>() {
                    @Override
                    public boolean filter(Post post) throws Exception {
                        return post.getDepth() == 1;
                    }
                })
                .map(new MapFunction<Post, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Post post) throws Exception {
                        return new Tuple2<>(FlinkUtils.getTimeSlot(post), 1);
                    }
                })
                .keyBy(t -> t.f0)
                .timeWindow(Time.hours(24))
                .sum(1);

        //day.print();


        /* aggregation by 1 day
           tumbling window all
           custom function apply
        */
        DataStream<Tuple2<Long, Map<Integer,Integer>>> dayStat = day
                .timeWindowAll(Time.hours(24))
                .apply(new AggregateTimeSlot())
                .setParallelism(1);
        //dayStat.print();

        dayStat.writeAsText("result/query2_day").setParallelism(1);
        //dayStat.writeAsCsv("result/Query2/query2_day.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        /* aggregation by 7 days
           sliding window all
           custom fuction apply
         */
        DataStream<Tuple2<Long, Map<Integer, Integer>>> weekStat = day
                .timeWindowAll(Time.days(7), Time.hours(24))
                .apply(new AggregateTimeSlotSliding())
                .setParallelism(1);
        //weekStat.print();


        weekStat.writeAsText("result/query2_week").setParallelism(1);
        //weekStat.writeAsCsv("result/query2_week.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);



        /* aggregation by 30 days
           sliding window all
           custom function apply
         */
        DataStream<Tuple2<Long, Map<Integer, Integer>>> monthStat = day
                .timeWindowAll(Time.days(30), Time.days(1))
                .apply(new AggregateTimeSlotSliding())
                .setParallelism(1);
        //weekStat.print();


        monthStat.writeAsText("result/query2_month").setParallelism(1);
        //monthStat.writeAsCsv("result/query2_month.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        environment.execute("Query2");
    }
}
