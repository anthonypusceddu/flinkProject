package utils;

import model.Score;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.ArrayList;

public class Serializer {

    public static String serialize_score(Score score){
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        String s=null;
        try {
            s = mapper.writeValueAsString( score);
        } catch (IOException e) {
            System.out.println("error");
        }
        return s;
    }
}
