package model;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class PostDeserializer implements DeserializationSchema<Post> {

    @Override
    public Post deserialize(byte[] bytes) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Post post = null;
        try {
            post = mapper.readValue(bytes, Post.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return post;
    }

    @Override
    public boolean isEndOfStream(Post post) {
        return false;
    }

    @Override
    public TypeInformation<Post> getProducedType() {
        return null;
    }
}
