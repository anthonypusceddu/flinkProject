package utils;

import model.ArticleRank;
import model.ArticleRankSchema;
import model.Post;
import model.PostSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaUtils {

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

    public static FlinkKafkaProducer<ArticleRank> createStringProducer(String topic, String kafkaAddress) {
        return new FlinkKafkaProducer<>(kafkaAddress, topic, new ArticleRankSchema());
    }
}
