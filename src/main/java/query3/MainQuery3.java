package query3;

import model.Post;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import query3.operators.CountReplyDepth3;
import utils.Config;
import utils.FlinkUtils;
import utils.KafkaUtils;
import utils.PostTimestampAssigner;

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
        DataStream<Tuple3<Long, Integer, Integer>> numberOfDepth2Comment = getData
                .filter(new FilterFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<Integer, Integer, Integer, Integer, Integer> tuple5) throws Exception {
                        return tuple5.f1==3;
                    }
                })
                .keyBy(t -> t.f3)
                .timeWindow(Time.hours(24))
                .apply(new CountReplyDepth3());
        numberOfDepth2Comment.print();

        //( UserId, Depth, Like, InReplyTo, CommentID) join ( Ts, CommentIdOfLevel2, countOfLevel3Comment )
                                     // on CommentID = CommentIdOfLevel2

        DataStream<Tuple3<Long, Integer,Integer>> depth2Comment = getData
                .filter(new FilterFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<Integer, Integer, Integer, Integer, Integer> tuple5) throws Exception {
                        return tuple5.f1==2;
                    }
                })
                .keyBy(t->t.f4)
                .timeWindow(Time.hours(24))
                .apply(new WindowFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple3<Long, Integer, Integer>, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Integer, Integer>> iterable, Collector<Tuple3<Long, Integer, Integer>> out) throws Exception {
                        out.collect(new Tuple3<>(timeWindow.getStart(),key,1));
                    }
                });
        DataStream<Tuple3<Long,Integer,Integer>> prova = depth2Comment
                .join(numberOfDepth2Comment)
                .where(t -> t.f1).equalTo(t1 -> t1.f1)
                .window(TumblingEventTimeWindows.of(Time.hours(24)))
                .apply(new JoinFunction<Tuple3<Long, Integer, Integer>, Tuple3<Long, Integer, Integer>, Tuple3<Long, Integer, Integer>>() {
                    @Override
                    public Tuple3<Long, Integer, Integer> join(Tuple3<Long, Integer, Integer> t, Tuple3<Long, Integer, Integer> t1) throws Exception {
                        return new Tuple3<>();
                    }
                });
        //numberOfComment.print();

        environment.execute("Query3");

    }
}
