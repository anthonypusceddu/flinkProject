package flink.query1;


import flink.query1.operators.TopN;
import flink.query1.operators.TopN_Sliding;
import org.apache.flink.core.fs.FileSystem;
import utils.Config;
import model.Post;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.FlinkUtils;
import utils.KafkaUtils;
import utils.PostTimestampAssigner;

import java.util.*;

public class MainQuery1 {

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
          map to (articleID, occurrencies set to 1)
          keyBy sets the key
          tumbling window of 1 hour / 24 hours / 7 days
          sum the occurrencies of articleID
         */
        DataStream<Tuple2<String, Integer>> hour = stringInputStream
                .map(new MapFunction<Post, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Post post) throws Exception {
                        return new Tuple2<>(post.getArticleId(), 1);
                    }
                })
                .keyBy(t -> t.f0)
                .timeWindow(Time.hours(1))
                //.window(SlidingEventTimeWindows.of(Time.hours(1),Time.seconds(10)) )
                .sum(1);

        //hour.print();

        //Scrittura su file -- cambiare nome del file _1hour, _24hours , _7days
        //a seconda della finestra
        //hour.writeAsCsv("result/Query1/query1_1hour.csv", FileSystem.WriteMode.NO_OVERWRITE).setParallelism(1);


        DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> hourStat = hour
                .timeWindowAll(Time.hours(1))
                .apply(new TopN())
                .setParallelism(1);
        //hourStat.print();
        hourStat.writeAsCsv("result/Query1/1HourTopN.txt", FileSystem.WriteMode.OVERWRITE, "\n", ",").setParallelism(1);

        DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> DayStat = hour
                .timeWindowAll(Time.hours(24),Time.hours(1))
                .apply(new TopN_Sliding())
                .setParallelism(1);

        //DayStat.print();
        DayStat.writeAsCsv("result/Query1/1DayTopN.txt", FileSystem.WriteMode.OVERWRITE, "\n", ",").setParallelism(1);

        DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> WeekStat = hour
                .timeWindowAll(Time.days(7),Time.hours(24))
                .apply(new TopN_Sliding())
                .setParallelism(1);
        //WeekStat.print();
        WeekStat.writeAsText("result/Query1/1WeekTopN.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
       // WeekStat.writeAsCsv("result/Query1/1WeekTopN.csv", FileSystem.WriteMode.OVERWRITE, "\n", ",").setParallelism(1);



        //.addSink(flinkKafkaProducer);*/
        //environment.setParallelism(1);
        environment.execute("Query1");
    }




}

