package kafkaProducer;

import com.opencsv.CSVReader;
import utils.Config;
import model.Post;
import model.PostSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class TupleProducer {

    private final static String TOPIC = Config.TOPIC;
    //"localhost:9092";
    //"localhost:9092,localhost:9093,localhost:9094";

    private static Producer<Long, Post> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Config.kafkaBrokerList);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                PostSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static Post lineToPost(String[] values) {
        Post t = new Post();

        t.setApproveDate(Long.valueOf(values[0])*1000);
        t.setArticleId(values[1]);
        t.setArticleWordCount(Integer.valueOf(values[2]));
        t.setCommentID(Integer.valueOf(values[3]));
        t.setCommentType(values[4]);
        t.setCreateDate(Long.valueOf(values[5])*1000);
        t.setDepth(Integer.valueOf(values[6]));
        t.setEditorsSelection(Boolean.valueOf(values[7]));
        t.setInReplyTo(Integer.valueOf(values[8]));
        t.setParentUserDispalyName(values[9]);
        try{
            t.setRecommendations(Integer.valueOf(values[10]));
        }catch ( NumberFormatException e){
            t.setRecommendations(-1);
        }
        //t.setRecommendations(Integer.valueOf(values[10]));
        t.setSectionName(values[11]);
        t.setUserDisplayName(values[12]);
        try{
            t.setUserID(Integer.valueOf(values[13]));
        }catch ( NumberFormatException e){
            t.setUserID(-1);
        }
        //t.setUserID(Integer.valueOf(values[13]));
        t.setUserLocation(values[14]);

        return t;
    }

    private static ArrayList<Post> readCSV(){
        ArrayList<Post> records = new ArrayList<>();

        try {
            Reader reader = Files.newBufferedReader(Paths.get(Config.filename));
            CSVReader csvReader = new CSVReader(reader);
            csvReader.skip(1);
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                Post post = lineToPost(line);
                records.add(post);
            }
            reader.close();
            csvReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }


    private static void runProducer() {
        final Producer<Long, Post> producer = createProducer();

        ArrayList<Post> postList = readCSV();

        long r = 999/1000 ;
        long r1 = 34722/100000000;
        long r2 = 28935/1000000000;

        Post first = postList.get(0);
        final ProducerRecord<Long, Post> primo = new ProducerRecord<>(Config.TOPIC, (long)0, first);
        producer.send(primo);
        long current_time = first.getCreateDate();
        try {
            for (int i = 0; i<postList.size() -1; i++) {
                //Post current = postList.get(i);
                Post next = postList.get(i+1);
                long rate = next.getCreateDate() - current_time; // millisecondi
                //System.out.println(time);
                long scaledRate = (rate*28935)/1000000000;
                current_time = next.getCreateDate();
                //next.setCreateDate(next.getCreateDate()- scaledTime);

                System.out.println((scaledRate));
                Thread.sleep(scaledRate);
                final ProducerRecord<Long, Post> record = new ProducerRecord<>(Config.TOPIC, (long)i, next);
                producer.send(record);
                //RecordMetadata metadata = producer.send(record).get();
              /*  System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), rate);*/
            }
        } catch (InterruptedException e/*| ExecutionException e*/) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) {
        TupleProducer.runProducer();
    }
}
