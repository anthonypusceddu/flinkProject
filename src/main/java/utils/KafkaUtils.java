package utils;

import model.Post;
import model.PostSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

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

}
