package query3;

import model.Post;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import query3.operators.CountDirectLike;
import query3.operators.CountIndirectComment;
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

        // a: numero di like ai commenti diretti di ogni utente
        // b : numero di commenti indiretti collegati ai commenti di ogni utente


        DataStream<Tuple6<Integer,String, Boolean,Integer,Integer,Integer>> getData = stringInputStream
                .map(new MapFunction<Post, Tuple6<Integer,String,Boolean,Integer,Integer,Integer>>() {
                    @Override
                    public Tuple6<Integer,String, Boolean,Integer,Integer,Integer> map(Post post) throws Exception {
                        if (post.isEditorsSelection() && post.getCommentType().equals("comment"))
                            post.setRecommendations(post.getRecommendations()+post.getRecommendations()*10/100);
                        return new Tuple6<>(post.getUserID(),post.getCommentType(), post.isEditorsSelection(),post.getRecommendations(),post.getInReplyTo(),post.getCommentID());
                    }
                });

        double wa=0.3;
        double wb=0.7;
        //return -> Tuple3<window-key-#like direct comment>
        //key is idUser
        DataStream<Tuple3<Long,Integer, Integer>> a =getData.filter(t->t.f1.equals("comment"))
                .keyBy(t -> t.f0)
                .timeWindow(Time.hours(1))
                .process(new CountDirectLike());

        DataStream<Tuple4<Long,Integer, Integer,Integer>> b =getData.
                filter(t->t.f1.equals("userReply"))
                .map((new MapFunction<Tuple6<Integer,String, Boolean,Integer,Integer,Integer>, Tuple4<Integer,Integer,Integer,Integer>>() {
                    @Override
                    public Tuple4<Integer,Integer,Integer,Integer> map(Tuple6<Integer,String, Boolean,Integer,Integer,Integer> post) throws Exception {
                        return new Tuple4<>(post.f0,post.f4,post.f5,1);
                    }
                }))
                .keyBy(t -> t.f2)
                .timeWindow(Time.hours(1))
                //.window(SlidingEventTimeWindows.of(Time.hours(1),Time.seconds(10)) )

                //fare una reduce per avere : userid, id commento , numero totale di id commenti
                // sommare i commenti che hanno id uguali a quelli indiretti
                .apply();

    }



}
