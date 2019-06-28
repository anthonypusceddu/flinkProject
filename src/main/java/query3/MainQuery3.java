package query3;

import model.Comment;
import model.Post;
import model.State;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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



        DataStream<HashMap> numberOfDepth2Comment = getData
                .process(new ProcessFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, HashMap>() {
                    private ValueState<State> valueState;

                    //initialize state
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.valueState= getRuntimeContext().getState(new ValueStateDescriptor<>("myState",State.class));
                    }

                    @Override
                    //input ( UserId, Depth, Like, InReplyTo, CommentID)
                    public void processElement(Tuple5<Integer, Integer, Integer, Integer, Integer> value, Context ctx, Collector<HashMap> out) throws Exception {

                        Comment comment= new Comment(value.f0,value.f4,value.f1,value.f3,value.f2,0);
                        switch (value.f1){
                            case 1:
                                valueState.value().updateHashmapDirect(comment.getCommentID(),comment);
                                break;
                            case 2:
                                this.valueState.value().updateHashmapIndirect(comment.getCommentID(),comment.getInReplyTo());
                                this.valueState.value().add(comment.getInReplyTo());
                                break;
                            case 3:
                                Integer commentIdDepth1=valueState.value().getIdIndirectHash(comment.getInReplyTo());
                                if( commentIdDepth1 != null)
                                    this.valueState.value().add(commentIdDepth1);

                                break;
                        }
                        if (valueState.value().isFirstTimeInWindow()) {
                            valueState.value().setFirstTimeInWindow(false);
                            valueState.value().setTimestamp(ctx.timestamp());
                        }
                        else{
                            if (ctx.timestamp()-valueState.value().getTimestamp() >= 8640000){
                                out.collect(valueState.value().getHdirectComment());
                                valueState.clear();
                            }
                        }
                    }
                });

        environment.execute("Query3");

    }
}
