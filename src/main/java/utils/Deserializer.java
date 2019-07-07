package utils;

import model.Score;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class Deserializer {

    public static Score deserialize_score(String score_serialized){
        ObjectMapper mapper = new ObjectMapper();
        Score score=null;
        try {
            score= mapper.readValue(score_serialized,Score.class);
        } catch (IOException e) {
            System.out.println("error");
        }
        return score;

    }

}
