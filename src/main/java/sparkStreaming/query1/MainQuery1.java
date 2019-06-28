package sparkStreaming.query1;

/*import model.Post;
import model.PostDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.codehaus.jackson.map.deser.std.StringDeserializer;
import scala.Tuple2;
import utils.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.*;

public class MainQuery1 {


    private static JavaInputDStream<ConsumerRecord<String, Post>> getKafkaConnection(JavaStreamingContext streamingContext){

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", Config.kafkaBrokerList);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", PostDeserializer.class);
        kafkaParams.put("group.id", "prova");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList(Config.TOPIC);

        return KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );
       // stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
    }

    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("query1")
                .master("local[2]")
                //.config("sparkCore.some.config.option", "some-value")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));
        /*
        //JavaStreamingContext jssc = new JavaStreamingContext(ctx, Durations.seconds(1));

        JavaInputDStream<ConsumerRecord<String, Post>> stream  = MainQuery1.getKafkaConnection(jssc);
        JavaPairDStream<String, Integer> count = stream
                .mapToPair(record -> new Tuple2<>(record.value().getArticleId(), 1))
                .reduceByKeyAndWindow(Integer::sum, Durations.minutes(60), Durations.minutes(60));

        count.print();
                //.map(record -> record.value());
    }
}*/
