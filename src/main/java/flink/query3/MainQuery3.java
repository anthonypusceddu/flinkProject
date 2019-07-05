package flink.query3;

import flink.query3.operators.MyProcessFunction;
import model.Comment;
import model.Post;
import model.State;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import utils.Config;
import utils.FlinkUtils;
import utils.KafkaUtils;
import utils.PostTimestampAssigner;

import java.io.IOException;
import java.util.HashMap;

public class MainQuery3 {

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

        /* query starts here
           map to (UserID, depth, likes, commentToReply, CommentID)
         */
        DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> getData = stringInputStream
                .map(new MapFunction<Post, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Post post) throws Exception {
                        if (post.isEditorsSelection() && post.getCommentType().equals("comment"))
                            post.setRecommendations(post.getRecommendations() + post.getRecommendations() * 10 / 100);
                        return new Tuple5<>(post.getUserID(), post.getDepth(), post.getRecommendations(), post.getInReplyTo(), post.getCommentID());
                    }
                });
        //input ( UserId, Depth, Like, InReplyTo, CommentID)
        //return ( Ts, CommentIdOfLevel2, countOfLevel3Comment)



        DataStream<Tuple2<Long,HashMap>> popularUserMap = getData
                .filter( tuple -> tuple.f0!=-1 && tuple.f2 !=-1)
                .process(new MyProcessFunction());

        //popularUserMap.print();

        //popularUserMap.writeAsText("result/query3").setParallelism(1);
        popularUserMap.writeAsCsv("result/query3.csv", FileSystem.WriteMode.NO_OVERWRITE).setParallelism(1);

        environment.execute("Query3");

    }
}
