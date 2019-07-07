package flink.query3;

import flink.query3.operators.AggregateTimeSlot3;
import flink.query3.operators.MyProcessFunction;
import model.Post;
import model.Score;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import redis.RedisJava;
import redis.clients.jedis.Jedis;
import utils.Config;
import utils.FlinkUtils;
import utils.KafkaUtils;
import utils.PostTimestampAssigner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        //( UserId, Depth, Like, InReplyTo, CommentID)
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



        DataStream<Tuple2<Long, List<HashMap<Integer, Score>>>> popularUserMap = getData
                .filter( tuple -> tuple.f0!=-1 && tuple.f2 !=-1)
                .process(new MyProcessFunction());

        popularUserMap.print();
        environment.execute("Query3");

    }
}
