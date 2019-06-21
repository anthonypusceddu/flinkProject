package query2;

import model.Post;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.Config;
import utils.FlinkUtils;
import utils.KafkaUtils;

public class MainQuery2 {

    public static void main(String[] args) {
        //create environment
        StreamExecutionEnvironment environment = FlinkUtils.setUpEnvironment(args);
        //Create kafka consumer
        FlinkKafkaConsumer<Post> flinkKafkaConsumer = KafkaUtils.createStringConsumerForTopic(
                Config.TOPIC, Config.kafkaBrokerList, Config.consumerGroup);

        //stream data from kafka consumer
        DataStream<Post> stringInputStream = environment
                .addSource(flinkKafkaConsumer);


        //(Fascia Oraria, 1)
        stringInputStream
                .filter(new FilterFunction<Post>() {
                    @Override
                    public boolean filter(Post post) throws Exception {
                        if (post.getDepth() != 1)
                            return false;
                        return true;
                    }
                })
                .map(new MapFunction<Post, Tuple2<Integer,Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Post post) throws Exception {
                        return new Tuple2<>(FlinkUtils.getTimeSlot(post), 1);
                    }
                });
    }
}
